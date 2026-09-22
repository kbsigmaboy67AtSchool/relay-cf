/**
 * Universal WSS Relay v2 — Cloudflare Workers + Durable Objects
 */
const MAX_VARS = 128, DEFAULT_MAX = 128, RATE_WINDOW = 10000, RATE_MAX = 120, MAX_MSG = 1048576;

export class Room {
  constructor(state, env) {
    this.state = state;
    this.sessions = new Map();
    this.vars = new Map();
  }
  broadcast(data, except, echo) {
    for (const [ws, meta] of this.sessions) {
      if (ws === except && !echo) continue;
      try { if (ws.readyState === 1) ws.send(data); } catch (_) {}
    }
  }
  rateOk(meta) {
    const now = Date.now();
    if (!meta.rate || now - meta.rate.t > RATE_WINDOW) meta.rate = { t: now, n: 0 };
    meta.rate.n++;
    return meta.rate.n <= RATE_MAX;
  }
  cloudSnapshot(ws) {
    const lines = [];
    for (const [name, value] of this.vars) lines.push(JSON.stringify({ t: "set", method: "set", name, value }));
    if (lines.length) try { ws.send(lines.join("\n")); } catch (_) {}
  }
  handleCloud(ws, meta, raw) {
    let msg;
    try { msg = JSON.parse(raw); } catch { try { ws.close(4000, "invalid json"); } catch (_) {} return; }
    const method = msg.t || msg.method;
    if (method === "handshake") {
      const user = String(msg.user || "").slice(0, 32);
      if (!user) { try { ws.close(4002, "username"); } catch (_) {} return; }
      meta.user = user; meta.handshook = true;
      this.cloudSnapshot(ws);
      try { ws.send(JSON.stringify({ t: "_hello", id: meta.id, n: this.sessions.size, mode: "cloud", user })); } catch (_) {}
      return;
    }
    if (!meta.handshook) { try { ws.close(4000, "handshake required"); } catch (_) {} return; }
    if (method === "set" || method === "create") {
      if (typeof msg.name !== "string" || msg.name.length < 1 || msg.name.length > 128) return;
      if (typeof msg.value !== "string" && typeof msg.value !== "number") return;
      if (!this.vars.has(msg.name) && this.vars.size >= MAX_VARS) return;
      this.vars.set(msg.name, msg.value);
      this.broadcast(JSON.stringify({ t: "set", method: "set", name: msg.name, value: msg.value }), ws, false);
    } else if (method === "delete") {
      if (!this.vars.has(msg.name)) return;
      this.vars.delete(msg.name);
      this.broadcast(JSON.stringify({ t: "delete", method: "delete", name: msg.name }), ws, false);
    } else if (method === "rename") {
      if (!this.vars.has(msg.name) || this.vars.has(msg.new_name)) return;
      const val = this.vars.get(msg.name);
      this.vars.delete(msg.name); this.vars.set(msg.new_name, val);
      this.broadcast(JSON.stringify({ t: "rename", method: "rename", name: msg.name, new_name: msg.new_name }), ws, false);
    }
  }
  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Universal WSS Relay v2 (CF) — ?mode=bus|cloud\n", { headers: { "content-type": "text/plain" } });
    }
    const url = new URL(request.url);
    const presence = url.searchParams.get("presence") === "1";
    const echo = url.searchParams.get("echo") === "1";
    const mode = url.searchParams.get("mode") === "cloud" ? "cloud" : "bus";
    const max = parseInt(url.searchParams.get("max") || String(DEFAULT_MAX), 10) || DEFAULT_MAX;
    if (this.sessions.size >= max) return new Response("room full", { status: 503 });
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    const id = crypto.randomUUID().slice(0, 8);
    const meta = { id, echo, presence, mode, user: "", handshook: mode !== "cloud", rate: null };
    this.sessions.set(server, meta);
    try { server.send(JSON.stringify({ t: "_hello", id, n: this.sessions.size, mode })); } catch (_) {}
    if (presence) this.broadcast(JSON.stringify({ t: "_join", id, user: "", n: this.sessions.size }), server, false);
    server.addEventListener("message", (event) => {
      if (!this.rateOk(meta)) { try { server.send(JSON.stringify({ t: "_err", error: "rate_limited" })); } catch (_) {} return; }
      const data = event.data;
      if (typeof data !== "string") {
        if (mode === "cloud") return;
        this.broadcast(data, server, meta.echo);
        return;
      }
      if (data.length > MAX_MSG) return;
      if (mode === "cloud") {
        for (const line of data.split("\n")) if (line.trim()) this.handleCloud(server, meta, line.trim());
        return;
      }
      if (data.startsWith("{")) {
        try {
          const msg = JSON.parse(data);
          if ((msg.t || msg.method) === "handshake") { meta.user = String(msg.user || "").slice(0, 32); return; }
        } catch (_) {}
      }
      this.broadcast(data, server, meta.echo);
    });
    const leave = () => {
      if (!this.sessions.has(server)) return;
      this.sessions.delete(server);
      if (meta.presence) this.broadcast(JSON.stringify({ t: "_leave", id: meta.id, user: meta.user, n: this.sessions.size }), null, false);
      if (this.sessions.size === 0) this.vars.clear();
    };
    server.addEventListener("close", leave);
    server.addEventListener("error", leave);
    return new Response(null, { status: 101, webSocket: client });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/" || url.pathname === "/health") {
      return new Response("Universal WSS Relay v2 — path = room · mode=bus|cloud\n", { headers: { "content-type": "text/plain" } });
    }
    const roomId = url.pathname.replace(/^\/+/, "") || "default";
    return env.ROOMS.get(env.ROOMS.idFromName(roomId)).fetch(request);
  },
};
