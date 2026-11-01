// Cloudflare Worker: fixed URL -> Firebase me likha current tunnel URL par proxy.
// Variable: FIREBASE_URL = https://toonflix-wed-default-rtdb.firebaseio.com
let cached = { url: "", t: 0 };

async function getOrigin(env, force = false) {
  if (!force && cached.url && Date.now() - cached.t < 30000) return cached.url;
  const r = await fetch(`${env.FIREBASE_URL.replace(/\/$/, "")}/stream_origin.json`);
  const d = await r.json();
  if (d && d.url) cached = { url: d.url.replace(/\/$/, ""), t: Date.now() };
  return cached.url;
}

export default {
  async fetch(request, env) {
    const u = new URL(request.url);
    const hdrs = new Headers(request.headers);
    hdrs.delete("host");
    const send = async (origin) =>
      fetch(origin + u.pathname + u.search, {
        method: request.method, headers: hdrs, redirect: "manual",
        body: ["GET", "HEAD"].includes(request.method) ? undefined : request.body,
      });
    try {
      let res = await send(await getOrigin(env));
      if (res.status >= 520 || res.status === 502 || res.status === 530) {
        res = await send(await getOrigin(env, true));   // tunnel badal gaya ho to dobara
      }
      return res;
    } catch (e) {
      try { return await send(await getOrigin(env, true)); }
      catch { return new Response("Server abhi offline hai, thodi der baad try karo.", { status: 503 }); }
    }
  },
};
