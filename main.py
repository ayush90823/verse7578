"""
ToonflixHindi - Telegram Link Generator + Stream Server

Bot ko video/file bhejo -> bot use STORAGE_CHANNEL mein copy karta hai aur
Stream / Embed / Download links wapas Telegram par bhej deta hai.
Koi database nahi, koi Firebase nahi, links kahin save nahi hote.
(Links isliye chalte hain kyunki file STORAGE_CHANNEL mein rehti hai.)

Saari settings ENVIRONMENT VARIABLES se aati hain (code mein secrets mat likho):

    BOT_TOKEN         @BotFather se
    API_ID            my.telegram.org se (number)
    API_HASH          my.telegram.org se
    STORAGE_CHANNEL   channel ID (-100 ke saath), bot us channel ka admin ho
    SECRET_KEY        lambi random key (kam se kam 32 chars). Purane links chalane hain
                      to wahi purani key daalo, naye setup ke liye random key banao:
                      python -c "import secrets; print(secrets.token_urlsafe(48))"
    BASE_URL          public https URL, end mein "/" nahi
    ALLOWED_USERS     comma separated Telegram user IDs
    ALLOW_ALL_USERS   "true" = sab use kar sakte hain (sirf testing)
    PORT              (optional) platform deta hai
    BRAND_NAME        (optional) default: ToonflixHindi
    ROUTE_PREFIX      (optional) default: toonflix  (badloge to purane links toot jayenge)
"""
import io
import os
import re
import hmac
import hashlib
import logging
import mimetypes
import urllib.parse
import asyncio
import math
import sys
import time
import json
import urllib.request
from collections import OrderedDict
from contextlib import asynccontextmanager
from datetime import timedelta
from html import escape as html_escape

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse, HTMLResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from pyrogram import Client
from pyrogram.errors import FloodWait
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import RetryAfter
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# ── ENV CONFIG ────────────────────────────────────────


def _env_int(name: str, default: int = 0) -> int:
    raw = (os.getenv(name) or "").strip()
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


PORT             = _env_int("PORT", 8000)
BOT_TOKEN        = (os.getenv("BOT_TOKEN") or "").strip()
API_ID           = _env_int("API_ID", 0)
API_HASH         = (os.getenv("API_HASH") or "").strip()
STORAGE_CHANNEL  = _env_int("STORAGE_CHANNEL", 0)
SECRET_KEY       = (os.getenv("SECRET_KEY") or "").strip()
BASE_URL         = (os.getenv("BASE_URL") or f"http://localhost:{PORT}").strip().rstrip("/")
ALLOWED_USERS    = (os.getenv("ALLOWED_USERS") or "").strip()
ALLOW_ALL_USERS  = _env_bool("ALLOW_ALL_USERS", False)
BRAND_NAME       = (os.getenv("BRAND_NAME") or "ToonflixHindi").strip()
ROUTE_PREFIX     = (os.getenv("ROUTE_PREFIX") or "toonflix").strip().strip("/")
# Firebase sirf tunnel ka current address Worker tak pahunchane ke liye (links/files yahan save nahi hote)
FIREBASE_URL     = (os.getenv("FIREBASE_URL") or "").strip().rstrip("/")
ORIGIN_URL       = (os.getenv("ORIGIN_URL") or "").strip().rstrip("/")  # workflow tunnel URL set karta hai
# Purane links mein jo prefix the (comma se alag), jaise: toonflixhindi,toonflix
OLD_ROUTE_PREFIXES = [x.strip().strip("/") for x in (os.getenv("OLD_ROUTE_PREFIXES") or "").split(",") if x.strip()]
# Player site ke origin (comma se alag), jaise: https://meri-site.com  (khali = sab allowed)
PLAYER_ORIGINS   = {x.strip().rstrip("/") for x in (os.getenv("PLAYER_ORIGINS") or "").split(",") if x.strip()}

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
# httpx/httpcore INFO logs mein poora bot token (URL ke andar) chhap dete hain - inhe chup rakho
for _noisy in ("httpx", "httpcore", "hpack"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

# Pyrogram client validate_config() ke baad main() mein banta hai (placeholder values se bachne ke liye)
pyro: Client = None  # type: ignore

# Sirf stream ke liye chhota LRU cache (RAM bounded). Ye data save nahi hota, restart par khali.
message_cache: OrderedDict = OrderedDict()
MESSAGE_CACHE_LIMIT = 50


def build_pyro() -> Client:
    return Client(
        "stream_session",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        in_memory=True,
        no_updates=True,
        sleep_threshold=60,
        max_concurrent_transmissions=4,
    )


# ── TUNNEL ORIGIN PUBLISH (Worker isse padhta hai) ────

def _put_origin_sync():
    req = urllib.request.Request(
        f"{FIREBASE_URL}/stream_origin.json",
        data=json.dumps({"url": ORIGIN_URL, "time": int(time.time())}).encode(),
        method="PUT",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.status


async def publish_origin():
    """Tunnel ka current address Firebase mein likhta hai taaki Worker usko padh sake."""
    if not (FIREBASE_URL and ORIGIN_URL):
        logger.warning("FIREBASE_URL/ORIGIN_URL set nahi - origin publish skip (Worker ko naya tunnel URL nahi milega)")
        return
    try:
        status = await asyncio.to_thread(_put_origin_sync)
        logger.info(f"Origin publish status: {status} -> {ORIGIN_URL}")
    except Exception as e:
        logger.error(f"publish_origin error: {e}")


# ── HELPERS (BRANDING & LINK SIGNING) ─────────────────

def allowed_user_ids():
    return {u.strip() for u in ALLOWED_USERS.split(",") if u.strip()}


def is_allowed(user_id):
    # User restriction abhi band hai: ALLOWED_USERS khali ho to sab use kar sakte hain.
    # Wapas lagana ho to env mein ALLOWED_USERS=id1,id2 set kar do.
    allowed = allowed_user_ids()
    if not allowed:
        return True
    return str(user_id) in allowed


def generate_code(msg_id, filename):
    # HMAC-SHA256; 32 hex chars
    msg = f"{ROUTE_PREFIX}:{msg_id}:{filename}".encode()
    return hmac.new(SECRET_KEY.encode(), msg, hashlib.sha256).hexdigest()[:32]


def make_stream_link(msg_id, filename):
    safe = urllib.parse.quote(filename)
    code = generate_code(msg_id, filename)
    return f"{BASE_URL}/{ROUTE_PREFIX}/dl/{msg_id}/{safe}?code={code}"


def make_download_link(msg_id, filename):
    safe = urllib.parse.quote(filename)
    code = generate_code(msg_id, filename)
    return f"{BASE_URL}/{ROUTE_PREFIX}/dl/{msg_id}/{safe}?code={code}&dl=1"


def make_embed_link(msg_id, filename):
    safe = urllib.parse.quote(filename)
    code = generate_code(msg_id, filename)
    return f"{BASE_URL}/{ROUTE_PREFIX}/watch/{msg_id}/{safe}?code={code}"


def verify_code(msg_id, filename, code):
    return hmac.compare_digest(generate_code(msg_id, filename), code or "")


def validate_config():
    """Startup par zaroori settings check karo; galat ho to saaf message ke saath band."""
    errors = []

    if not BOT_TOKEN or ":" not in BOT_TOKEN or "YAHAN" in BOT_TOKEN.upper():
        errors.append("BOT_TOKEN set nahi hai ya galat hai (format: 123456:ABC...)")
    if API_ID <= 0:
        errors.append("API_ID set nahi hai (my.telegram.org wala number)")
    if not API_HASH or "YAHAN" in API_HASH.upper():
        errors.append("API_HASH set nahi hai")
    if not STORAGE_CHANNEL:
        errors.append("STORAGE_CHANNEL set nahi hai (format: -100xxxxxxxxxx)")
    if not SECRET_KEY:
        errors.append("SECRET_KEY set karo")
    if not BASE_URL.startswith(("http://", "https://")):
        errors.append("BASE_URL http:// ya https:// se shuru hona chahiye")

    if errors:
        for e in errors:
            logger.error("CONFIG ERROR: %s", e)
        sys.exit(1)

    # Warnings (band nahi karenge, taaki purani key se links chalte rahein)
    if len(SECRET_KEY) < 32:
        logger.warning(
            "SECRET_KEY chhoti/kamzor hai (%d chars). Koi guess karke links bana sakta hai. "
            "Naya setup hai to 32+ random chars use karo.", len(SECRET_KEY)
        )
    if SECRET_KEY.lower() in (BRAND_NAME.lower(), ROUTE_PREFIX.lower(), "toonflixhindi"):
        logger.warning("SECRET_KEY brand name jaisi hai - jaldi se jaldi badlo (purane links toot jayenge).")
    if BASE_URL.startswith("http://localhost"):
        logger.warning("BASE_URL abhi localhost hai - apna public https URL set karo")
    if not allowed_user_ids():
        logger.warning("ALLOWED_USERS khali hai - bot sabke liye khula hai")


def _retry_seconds(e: RetryAfter) -> int:
    ra = e.retry_after
    if isinstance(ra, timedelta):
        return int(ra.total_seconds())
    return int(ra)


async def copy_with_floodwait(context, chat_id, from_chat_id, message_id, max_retries=10):
    """copy_message ko FloodControl (RetryAfter) par wait karke retry karta hai."""
    for attempt in range(max_retries):
        try:
            return await context.bot.copy_message(
                chat_id=chat_id,
                from_chat_id=from_chat_id,
                message_id=message_id,
            )
        except RetryAfter as e:
            wait_s = _retry_seconds(e) + 1
            logger.warning(f"FloodWait: {wait_s}s wait kar raha hoon (attempt {attempt + 1})")
            await asyncio.sleep(wait_s)
    raise RuntimeError("Max retries exceeded for copy_message (FloodWait loop).")


def get_extension(filename: str, fallback: str = "mp4") -> str:
    if filename and "." in filename:
        return filename.rsplit(".", 1)[-1].lower()
    return fallback


# ── PURANE LINKS -> NAYE LINKS CONVERTER ──────────────
# Purane links ka msg_id aur filename path mein hota hai; code naya bana dete hain.
ALL_PREFIXES = list(dict.fromkeys(OLD_ROUTE_PREFIXES + [ROUTE_PREFIX]))
OLD_LINK_RE = re.compile(
    r"https?://[^\s\"'<>]+?/(?:" + "|".join(re.escape(p) for p in ALL_PREFIXES) + r")/(dl|watch)/(\d+)/([^\s?\"'<>]+)(?:\?([^\s\"'<>]*))?"
)


def convert_old_links(text: str):
    """text ke andar jitne bhi purane links hain unhe naye signed links se badal deta hai."""
    count = 0

    def _sub(m):
        nonlocal count
        kind, msg_id, quoted_name, query = m.group(1), int(m.group(2)), m.group(3), m.group(4) or ""
        filename = urllib.parse.unquote(quoted_name)
        count += 1
        if kind == "watch":
            return make_embed_link(msg_id, filename)
        if re.search(r"(?:^|&)dl=1(?:&|$)", query):
            return make_download_link(msg_id, filename)
        return make_stream_link(msg_id, filename)

    return OLD_LINK_RE.sub(_sub, text), count


async def send_converted(update: Update, new_text: str, count: int):
    msg = update.message
    if count == 0:
        await msg.reply_text("Is mein koi purana link nahi mila.")
        return
    if len(new_text) <= 3500:
        await msg.reply_text(f"✅ {count} link(s) convert hue:\n\n{new_text}", disable_web_page_preview=True)
    else:
        bio = io.BytesIO(new_text.encode("utf-8"))
        bio.name = "new_links.txt"
        await msg.reply_document(bio, caption=f"✅ {count} link(s) convert hue.")


async def convert_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    text = update.message.text or ""
    if not any(f"/{p}/" in text for p in ALL_PREFIXES):
        return
    new_text, count = convert_old_links(text)
    if count:
        await send_converted(update, new_text, count)


async def convert_text_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document
    if doc.file_size and doc.file_size > 5 * 1024 * 1024:
        await update.message.reply_text("File bahut badi hai (max 5 MB).")
        return
    tg_file = await doc.get_file()
    raw = bytes(await tg_file.download_as_bytearray())
    new_text, count = convert_old_links(raw.decode("utf-8", errors="ignore"))
    await send_converted(update, new_text, count)


# ── BOT HANDLERS ──────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        await update.message.reply_text("⛔ Unauthorized.")
        return
    await update.message.reply_text(
        f"👋 *{BRAND_NAME} Link Bot*\n\n"
        "Koi bhi video ya file bhejo, bot Stream / Embed / Download links bana kar dega.\n"
        "Bot kuch save nahi karta, links sirf is chat mein milte hain.\n\n"
        "Purane file ka link dobara chahiye to:\n`/getlink <msg_id> <filename>`\n\n"
        "Purane links badalne hain? Unhe yahan paste karo ya .txt file bhejo, bot naye links bana dega.",
        parse_mode="Markdown",
    )


async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    msg = update.message

    # .txt file bheji to wo purane links ki list maani jayegi -> convert karke wapas bhejo
    if msg.document and (
        msg.document.mime_type == "text/plain"
        or (msg.document.file_name or "").lower().endswith(".txt")
    ):
        await convert_text_file(update, context)
        return

    file_obj, raw_name = None, ""

    if msg.video:
        file_obj, raw_name = msg.video, msg.video.file_name or ""
    elif msg.document:
        file_obj, raw_name = msg.document, msg.document.file_name or ""
    elif msg.audio:
        file_obj, raw_name = msg.audio, msg.audio.file_name or ""
    elif msg.video_note:
        file_obj = msg.video_note
    else:
        return

    filename = raw_name or f"video_{file_obj.file_unique_id}.mp4"
    processing = await msg.reply_text(f"⏳ {BRAND_NAME}: link ban raha hai...")
    try:
        forwarded = await copy_with_floodwait(
            context, chat_id=STORAGE_CHANNEL, from_chat_id=msg.chat_id, message_id=msg.message_id
        )
        storage_msg_id = forwarded.message_id
        stream_link = make_stream_link(storage_msg_id, filename)
        download_link = make_download_link(storage_msg_id, filename)
        embed_link = make_embed_link(storage_msg_id, filename)
        await processing.delete()
        await msg.reply_text(
            f"▶️ *Stream Link:*\n`{stream_link}`\n\n"
            f"🖼️ *Embed Link:*\n`{embed_link}`\n\n"
            f"⬇️ *Download Link:*\n`{download_link}`\n\n"
            f"🆔 `{storage_msg_id}`",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("▶️ Stream", url=stream_link)],
                [InlineKeyboardButton("🖼️ Embed", url=embed_link)],
                [InlineKeyboardButton("⬇️ Download", url=download_link)],
            ]),
        )
    except Exception as e:
        logger.error(f"handle_media error: {e}")
        try:
            await processing.edit_text("❌ Link nahi ban paya. Bot STORAGE_CHANNEL ka admin hai ya nahi check karo.")
        except Exception:
            pass


async def get_link_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update.effective_user.id):
        return
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Use: `/getlink <msg_id> <filename>`", parse_mode="Markdown")
        return
    try:
        msg_id = int(args[0])
    except ValueError:
        await update.message.reply_text("msg_id number hona chahiye.")
        return
    filename = " ".join(args[1:])
    await update.message.reply_text(
        f"🔗 *Secured Links:*\n`{make_stream_link(msg_id, filename)}`\n"
        f"`{make_embed_link(msg_id, filename)}`\n`{make_download_link(msg_id, filename)}`",
        parse_mode="Markdown",
    )


# ── FASTAPI SERVER WITH SINGLE CLIENT ─────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # App startup par global pyro client ko ek hi baar start karega
    await pyro.start()
    logger.info("Single Pyrogram Client successfully started!")
    try:
        chat_info = await pyro.get_chat(STORAGE_CHANNEL)
        logger.info(f"Connected and Cached Storage Channel: {chat_info.title}")
    except Exception as e:
        logger.error(f"Startup peer channel cache error: {e}")
    await publish_origin()
    yield
    await pyro.stop()


web_app = FastAPI(title=f"{BRAND_NAME} TG Stream Server", lifespan=lifespan)

web_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "HEAD", "OPTIONS"],
    allow_headers=["Range", "Content-Type", "Authorization"],
    expose_headers=["Content-Range", "Accept-Ranges", "Content-Length"],
)


@web_app.get("/")
async def index():
    return HTMLResponse(f"<html><body style='font-family:sans-serif;text-align:center;padding:80px;background:#000;color:#fff'><h1>🎬 {html_escape(BRAND_NAME)} Stream Server</h1><p>Active & Secured 🔒</p></body></html>")


# PLAYER API: purana link bhejo -> naya signed link milega (SECRET_KEY server par hi rehti hai)
@web_app.get("/api/convert")
async def api_convert(url: str, request: Request):
    origin = (request.headers.get("origin") or "").rstrip("/")
    if PLAYER_ORIGINS and origin not in PLAYER_ORIGINS:
        raise HTTPException(status_code=403, detail="Origin not allowed")
    if len(url) > 2000:
        raise HTTPException(status_code=400, detail="URL too long")
    new_url, count = convert_old_links(url)
    return {"url": new_url, "converted": count > 0}


# BRANDED ROUTE 1: WATCH PLAYER
# NOTE: FastAPI/Starlette path params ko pehle hi decode kar deta hai,
# isliye yahan dobara unquote() nahi karte (warna "%" wale filenames toot jaate the).
@web_app.api_route(f"/{ROUTE_PREFIX}/watch/{{msg_id}}/{{filename:path}}", methods=["GET", "HEAD"])
async def watch_file(msg_id: int, filename: str, code: str = ""):
    if not verify_code(msg_id, filename, code):
        raise HTTPException(status_code=403, detail="Invalid Link or Brand verification failed.")

    stream_url = f"/{ROUTE_PREFIX}/dl/{msg_id}/{urllib.parse.quote(filename)}?code={code}"
    safe_title = html_escape(filename)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>{safe_title}</title>
<style>body {{ background:#000; display:flex; justify-content:center; align-items:center; min-height:100vh; margin:0; color:#fff }} video {{ width:100%; max-width:960px }}</style>
</head>
<body><video src="{stream_url}" controls playsinline></video></body>
</html>"""
    return HTMLResponse(html)


# ── PARALLEL STREAM SETTINGS ──────────────────────────
SEG_CHUNKS = 2   # ek fetch task = 2 x 1MB (kam RAM plan ke liye)
PREFETCH   = 2   # kitne segments ek saath Telegram se aayenge
# RAM use ~ SEG_CHUNKS * PREFETCH MB per viewer (default ~4-6MB).


async def fetch_segment(message, seg_offset: int, seg_limit: int, retries: int = 2) -> bytes:
    """Telegram se seg_limit chunks (1MB each) lata hai, fail hone par retry."""
    last = None
    for attempt in range(retries + 1):
        try:
            parts = []
            async for c in pyro.stream_media(message, offset=seg_offset, limit=seg_limit):
                parts.append(c)
            return b"".join(parts)
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
            last = e
        except Exception as e:
            last = e
            await asyncio.sleep(0.3 * (attempt + 1))
    raise last


# BRANDED ROUTE 2: STREAM / DOWNLOAD
@web_app.api_route(f"/{ROUTE_PREFIX}/dl/{{msg_id}}/{{filename:path}}", methods=["GET", "HEAD"])
async def stream_file(msg_id: int, filename: str, request: Request, code: str = "", dl: int = 0):
    decoded = filename  # already decoded by Starlette

    if not verify_code(msg_id, decoded, code):
        raise HTTPException(status_code=403, detail="Access Denied: Link manipulation detected.")

    if msg_id in message_cache:
        message = message_cache[msg_id]
        message_cache.move_to_end(msg_id)  # LRU: recently used ko end pe rakho
    else:
        try:
            message = await pyro.get_messages(STORAGE_CHANNEL, msg_id)
            if not message or message.empty:
                raise ValueError("Empty message from Telegram")
            message_cache[msg_id] = message
            if len(message_cache) > MESSAGE_CACHE_LIMIT:
                message_cache.popitem(last=False)  # sabse purana entry hata do (RAM bounded)
        except Exception as e:
            logger.error(f"Media extraction crash for msg_id {msg_id}: {e}")
            raise HTTPException(status_code=404, detail="Media extraction failed from Telegram Storage.")

    media = message.video or message.document or message.audio or message.video_note
    if not media:
        raise HTTPException(status_code=404, detail="No media found in the specified storage ID.")

    file_size = getattr(media, "file_size", None) or 0
    if file_size <= 0:
        raise HTTPException(status_code=404, detail="File size unavailable for this media.")

    # mime_type None ho sakta hai -> filename se guess, warna video/mp4
    mime_type = (
        getattr(media, "mime_type", None)
        or mimetypes.guess_type(decoded)[0]
        or "video/mp4"
    )
    CHUNK_SIZE = 1 * 1024 * 1024

    # ---- Range parsing (suffix range "bytes=-N" bhi supported) ----
    range_header = request.headers.get("range")
    start = 0
    end = file_size - 1
    if range_header:
        try:
            spec = range_header.replace("bytes=", "").split(",")[0].strip()
            a, b = spec.split("-", 1)
            if a == "":                      # bytes=-N  -> last N bytes
                n = int(b)
                start = max(file_size - n, 0)
            else:
                start = int(a)
                if b != "":
                    end = min(int(b), file_size - 1)
        except Exception:
            raise HTTPException(status_code=416, detail="Bad Range header",
                                headers={"Content-Range": f"bytes */{file_size}"})
        if start >= file_size or start > end:
            raise HTTPException(status_code=416, detail="Range not satisfiable",
                                headers={"Content-Range": f"bytes */{file_size}"})

    content_length = end - start + 1
    offset = start // CHUNK_SIZE
    first_chunk_cut = start % CHUNK_SIZE
    limit = math.ceil((content_length + first_chunk_cut) / CHUNK_SIZE)

    download_filename = f"[{BRAND_NAME}] {decoded}"
    safe_filename = urllib.parse.quote(download_filename)

    response_headers = {
        "Content-Type": mime_type,
        "Accept-Ranges": "bytes",
        "Content-Disposition": f"{'attachment' if dl else 'inline'}; filename*=UTF-8''{safe_filename}",
        "Content-Length": str(content_length),
        "Cache-Control": "public, max-age=3600",
    }
    if range_header:
        response_headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

    status = 206 if range_header else 200

    # HEAD: sirf headers, Telegram se data fetch nahi karna
    if request.method == "HEAD":
        return Response(status_code=status, headers=response_headers)

    # ---- Parallel read-ahead plan ----
    # Pehla segment sirf 1 chunk (jaldi pehla byte), baaki SEG_CHUNKS ke segments.
    segs = []
    i = 0
    while i < limit:
        n = 1 if i == 0 else min(SEG_CHUNKS, limit - i)
        segs.append((offset + i, n))
        i += n

    async def generator():
        tasks = []
        next_i = 0
        bytes_sent = 0
        first = True
        try:
            while next_i < len(segs) and len(tasks) < PREFETCH:
                tasks.append(asyncio.create_task(fetch_segment(message, *segs[next_i])))
                next_i += 1
            while tasks:
                data = await tasks.pop(0)
                if next_i < len(segs):
                    tasks.append(asyncio.create_task(fetch_segment(message, *segs[next_i])))
                    next_i += 1
                if first:
                    data = data[first_chunk_cut:]
                    first = False
                remaining = content_length - bytes_sent
                if remaining <= 0:
                    break
                if len(data) > remaining:
                    data = data[:remaining]
                if not data:
                    break
                yield data
                bytes_sent += len(data)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Stream error msg_id {msg_id}: {e}")
        finally:
            for t in tasks:
                t.cancel()

    return StreamingResponse(generator(), status_code=status, headers=response_headers)


# ── MAIN RUNNER ───────────────────────────────────────

async def run_bot():
    app = Application.builder().token(BOT_TOKEN).build()
    await app.bot.delete_webhook(drop_pending_updates=True)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("getlink", get_link_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, convert_text_message))
    app.add_handler(MessageHandler(filters.VIDEO | filters.Document.ALL | filters.AUDIO | filters.VIDEO_NOTE, handle_media))
    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    return app


async def run_server():
    config = uvicorn.Config(web_app, host="0.0.0.0", port=PORT, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    global pyro
    validate_config()
    pyro = build_pyro()
    bot_app = await run_bot()
    try:
        await run_server()
    finally:
        await bot_app.updater.stop()
        await bot_app.stop()
        await bot_app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
