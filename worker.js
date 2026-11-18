/* Sora Truck 3D · เซิร์ฟเวอร์ห้องออนไลน์ (Cloudflare Worker + Durable Objects)
   1 ห้อง = 1 Durable Object (ชื่อห้อง = รหัสห้อง) · ส่งต่อสถานะรถของแต่ละคนให้คนอื่นในห้อง
   ใช้ WebSocket Hibernation API → ห้องที่ไม่มีข้อความไม่ถูกคิดเวลา · คำว่า "ping" ตอบอัตโนมัติไม่ปลุกห้อง */
const MAX_PEERS = 8, MAX_MSG = 64 * 1024;
// โดเมนที่อนุญาตให้ต่อ (sora-truck-online.pages.dev + ลิงก์พรีวิวของ Pages + ทดสอบในเครื่อง) — เพิ่มได้ทาง env ALLOWED_ORIGINS (คั่นด้วย ,)
const ORIGIN_OK = (o, env) => {
  if (!o) return false;
  const extra = String(env.ALLOWED_ORIGINS || "").split(",").map(s => s.trim()).filter(Boolean);
  if (extra.includes(o)) return true;
  try {
    const u = new URL(o);
    for (const d of ["sora-truck-online.pages.dev", "sora-project-truck3d.pages.dev"])
      if (u.hostname === d || u.hostname.endsWith("." + d)) return u.protocol === "https:";
    if (u.hostname === "localhost" || u.hostname === "127.0.0.1") return true;
  } catch (e) {}
  return false;
};

export class Room {
  constructor(state, env) {
    this.state = state; this.env = env;
    this.state.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }
  async fetch(req) {
    if (req.headers.get("Upgrade") !== "websocket") return new Response("expected websocket", { status: 426 });
    if (this.state.getWebSockets().length >= MAX_PEERS) return new Response("room full", { status: 503 });
    const pair = new WebSocketPair(); const [client, server] = Object.values(pair);
    const id = crypto.randomUUID().replace(/-/g, "").slice(0, 8);
    this.state.acceptWebSocket(server);
    server.serializeAttachment({ id, name: "", kind: "", ver: "", sc: "", hello: 0 });
    return new Response(null, { status: 101, webSocket: client });
  }
  peers(except) {
    const out = [];
    for (const w of this.state.getWebSockets()) { if (w === except) continue; const a = w.deserializeAttachment(); a && a.hello && !a.left && out.push(a); }
    return out;
  }
  bc(from, str) { for (const w of this.state.getWebSockets()) if (w !== from) { const a = w.deserializeAttachment(); if (a && a.hello && !a.left) try { w.send(str) } catch (e) {} } }
  async webSocketMessage(ws, msg) {
    if (typeof msg !== "string" || msg.length > MAX_MSG) return;
    let m; try { m = JSON.parse(msg) } catch (e) { return }
    if (!m || typeof m !== "object") return;
    const a = ws.deserializeAttachment() || {};
    if (m.t === "hello") {
      a.name = String(m.name || "ผู้เล่น").replace(/[<>]/g, "").slice(0, 24);
      a.kind = m.kind === "semi" ? "semi" : "full"; a.ver = String(m.ver || "").slice(0, 48); a.sc = String(m.sc || "").slice(0, 24);
      const first = !a.hello; a.hello = 1; ws.serializeAttachment(a);
      ws.send(JSON.stringify({ t: "welcome", id: a.id, peers: this.peers(ws), max: MAX_PEERS }));
      this.bc(ws, JSON.stringify(Object.assign({ t: first ? "join" : "info" }, a)));
      return;
    }
    if (!a.hello) return;
    if (m.t === "s") {           // สถานะรถ
      m.id = a.id;
      if ((m.k === "semi" || m.k === "full") && m.k !== a.kind) { a.kind = m.k; ws.serializeAttachment(a) }
      if (typeof m.sc === "string" && m.sc !== a.sc) { a.sc = m.sc.slice(0, 24); ws.serializeAttachment(a) }
      this.bc(ws, JSON.stringify(m));
      return;
    }
    if (m.t === "name") { a.name = String(m.name || "").replace(/[<>]/g, "").slice(0, 24) || a.name; ws.serializeAttachment(a); this.bc(ws, JSON.stringify(Object.assign({ t: "info" }, a))); }
  }
  async webSocketClose(ws, code) { this.leave(ws); try { ws.close(1000, "bye") } catch (e) {} }
  async webSocketError(ws) { this.leave(ws) }
  leave(ws) { const a = ws.deserializeAttachment() || {}; if (a.left) return; a.left = 1; try { ws.serializeAttachment(a) } catch (e) {} a.hello && this.bc(ws, JSON.stringify({ t: "leave", id: a.id })) }
}

export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    const m = u.pathname.match(/^\/room\/([A-Za-z0-9_-]{3,24})\/?$/);
    if (!m) return new Response("Sora Truck3D online server: OK", { headers: { "content-type": "text/plain; charset=utf-8" } });
    if (!ORIGIN_OK(req.headers.get("Origin"), env)) return new Response("origin not allowed", { status: 403 });
    const id = env.ROOMS.idFromName(m[1].toUpperCase());
    return env.ROOMS.get(id).fetch(req);
  }
};
