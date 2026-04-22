"""
Live ingest + HTTP server in one process.

  OPC UA PLC ──subscribe──▶ ClientManager ──▶ SQLite (samples, WAL)
                                 │
                                 └─▶ fan-out queue ──▶ SSE subscribers

Config
  connection.yaml — endpoint, auth, security. PLC_USER/PLC_PASS env vars
                    override auth mode at runtime (never committed).
  tags.yaml       — list of tags; hot-reloaded on mtime change, so edits
                    on disk OR via the dashboard's "Add tag" button take
                    effect without a process restart.

HTTP endpoints
  GET    /live/tags          → tag catalog + connection status
  POST   /live/tags          → add a new tag ({name, node, category, unit, min, max})
  DELETE /live/tags/{name}   → remove a tag
  GET    /live/browse        → walk the PLC's address space for Variable nodes
  GET    /live/history?tags=…&since=…&until=…&limit=…
                              → scrollback from SQLite
  GET    /live/stream        → Server-Sent Events stream of live samples
  GET    /                   → static files in this directory
"""
import asyncio
import contextlib
import json
import logging
import os
import sqlite3
import time
from collections import deque
from pathlib import Path
from typing import Any, Optional

import yaml
from aiohttp import web
from asyncua import Client, ua
from asyncua.common.node import Node

HERE = Path(__file__).parent
CONN_PATH = HERE / "connection.yaml"
TAGS_PATH = HERE / "tags.yaml"
DB_PATH = HERE / "data" / "plc.db"

LOG = logging.getLogger("live")

SSE_QUEUE_MAX = 500      # drop-oldest if a subscriber lags
TAG_WATCH_SEC = 2.0      # how often to re-check tags.yaml mtime
BROWSE_NODE_CAP = 500    # safety cap when walking the address space


# ── SQLite ────────────────────────────────────────────────────────────────

def init_db(path: Path):
    path.parent.mkdir(exist_ok=True)
    con = sqlite3.connect(path)
    con.execute("PRAGMA journal_mode = WAL")
    con.execute("PRAGMA synchronous = NORMAL")
    con.execute("""
        CREATE TABLE IF NOT EXISTS samples (
            ts      REAL NOT NULL,
            tag     TEXT NOT NULL,
            value   REAL NOT NULL,
            quality INTEGER NOT NULL DEFAULT 1
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_samples_tag_ts ON samples(tag, ts)")
    con.commit()
    return con


# ── Tag registry ──────────────────────────────────────────────────────────

def load_tags() -> list[dict]:
    """Read and validate tags.yaml. Returns a list of dicts."""
    raw = yaml.safe_load(TAGS_PATH.read_text()) or {}
    tags = raw.get("tags", []) or []
    by_name = {}
    for t in tags:
        for f in ("name", "node", "category", "unit"):
            if f not in t:
                raise ValueError(f"tag missing field {f}: {t}")
        if t["name"] in by_name:
            raise ValueError(f"duplicate tag name: {t['name']}")
        by_name[t["name"]] = t
    return tags


def save_tags(tags: list[dict]):
    """Atomic write to tags.yaml. Preserves header comment."""
    header = (
        "# OPC UA tag list. Managed by live_server.py (edits from the dashboard\n"
        "# land here too). Connection settings live in connection.yaml.\n\n"
    )
    tmp = TAGS_PATH.with_suffix(".yaml.tmp")
    tmp.write_text(header + yaml.safe_dump({"tags": tags}, sort_keys=False, allow_unicode=True))
    tmp.replace(TAGS_PATH)


def tag_from_payload(body: dict) -> dict:
    """Validate + normalize an incoming tag POST."""
    required = ("name", "node", "category", "unit")
    missing = [f for f in required if not body.get(f)]
    if missing:
        raise ValueError(f"missing required field(s): {', '.join(missing)}")
    out = {
        "name":     str(body["name"]).strip(),
        "node":     str(body["node"]).strip(),
        "category": str(body["category"]).strip() or "other",
        "unit":     str(body["unit"]).strip(),
        "min":      float(body.get("min", 0.0)),
        "max":      float(body.get("max", 100.0)),
    }
    if not out["name"]:
        raise ValueError("name must not be empty")
    if not out["node"]:
        raise ValueError("node must not be empty")
    if out["max"] <= out["min"]:
        out["max"] = out["min"] + 1.0
    return out


# ── OPC UA client manager ─────────────────────────────────────────────────

class SubHandler:
    """asyncua pushes datachange callbacks here from its own thread pool.
    We bounce samples onto the asyncio loop so the writer task owns the DB."""
    def __init__(self, loop, sink: asyncio.Queue, nodeid_to_name: dict[str, str]):
        self.loop = loop
        self.sink = sink
        self.nodeid_to_name = nodeid_to_name

    def datachange_notification(self, node, val, data):
        try:
            nid = node.nodeid.to_string()
            name = self.nodeid_to_name.get(nid)
            if name is None:
                return
            ts = time.time()
            mv = getattr(data, "monitored_item", None)
            if mv is not None and getattr(mv.Value, "SourceTimestamp", None):
                ts = mv.Value.SourceTimestamp.timestamp()
            quality = 1
            if mv is not None and mv.Value.StatusCode and not mv.Value.StatusCode.is_good():
                quality = 0
            sample = {"id": name, "ts": ts, "value": float(val), "q": quality}
            self.loop.call_soon_threadsafe(self._push_nowait, sample)
        except Exception as e:
            LOG.warning("datachange handler error: %s", e)

    def _push_nowait(self, sample):
        try:
            self.sink.put_nowait(sample)
        except asyncio.QueueFull:
            try:
                self.sink.get_nowait()
            except asyncio.QueueEmpty:
                pass
            self.sink.put_nowait(sample)


class ClientManager:
    """
    Owns the asyncua Client, the subscription, and the per-tag handle map.
    Offers .reconcile(tags) to sync the subscription against the desired tag set.
    Retries the underlying connection on any failure.
    """
    def __init__(self, conn: dict, app: web.Application):
        self.conn = conn
        self.app = app
        self.client: Optional[Client] = None
        self.sub = None
        self.handles: dict[str, Any] = {}        # name -> subscription handle
        self.nodeid_to_name: dict[str, str] = {}  # nodeid string -> tag name
        self.connected = False
        self.connect_error: Optional[str] = None
        self.auth_mode = "anonymous"
        self._reconcile_lock = asyncio.Lock()

    def _apply_auth(self, client: Client):
        """Env vars > connection.yaml. Mirror SubHandler.anonymous when neither set."""
        user = os.environ.get("PLC_USER") or (self.conn.get("auth") or {}).get("username")
        pwd  = os.environ.get("PLC_PASS") or (self.conn.get("auth") or {}).get("password")
        cfg_mode = (self.conn.get("auth") or {}).get("mode", "anonymous").lower()
        if user and pwd:
            client.set_user(user)
            client.set_password(pwd)
            self.auth_mode = "password"
        elif cfg_mode == "password":
            LOG.warning("auth mode=password but PLC_USER/PLC_PASS not set — falling back to anonymous")
            self.auth_mode = "anonymous"
        else:
            self.auth_mode = "anonymous"

    async def _apply_security(self, client: Client):
        policy = ((self.conn.get("security") or {}).get("policy") or "none").lower()
        if policy == "none":
            return
        cert = (self.conn.get("security") or {}).get("cert_path")
        key  = (self.conn.get("security") or {}).get("key_path")
        if not cert or not key:
            raise RuntimeError(f"security.policy={policy} requires cert_path and key_path")
        # e.g. "Basic256Sha256,SignAndEncrypt,/path/cert.der,/path/key.pem"
        policy_name = policy.replace("_", "").title() if policy != "basic256sha256" else "Basic256Sha256"
        await client.set_security_string(f"{policy_name},SignAndEncrypt,{cert},{key}")

    async def run(self):
        endpoint = self.conn["endpoint"]
        interval_ms = int(float(self.conn.get("sample_interval_s", 1.0)) * 1000)

        while True:
            try:
                LOG.info("connecting to %s", endpoint)
                self.connect_error = None
                client = Client(endpoint)
                self._apply_auth(client)
                await self._apply_security(client)
                self.client = client
                async with client:
                    handler = SubHandler(asyncio.get_running_loop(),
                                         self.app["ingest_q"],
                                         self.nodeid_to_name)
                    self.sub = await client.create_subscription(interval_ms, handler)
                    self.connected = True
                    LOG.info("connected (%s auth)", self.auth_mode)
                    # Subscribe to the current tag set
                    await self.reconcile(self.app["tags"])
                    # Block here; reconcile() is called externally on tag changes.
                    while True:
                        await asyncio.sleep(5.0)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.connected = False
                self.connect_error = str(e)
                self.sub = None
                self.handles.clear()
                self.nodeid_to_name.clear()
                LOG.warning("OPC UA connection failed (%s) — retrying in 5s", e)
                await asyncio.sleep(5.0)

    async def reconcile(self, tags: list[dict]):
        """Sync subscription state to match `tags`. Safe to call concurrently."""
        async with self._reconcile_lock:
            if not (self.connected and self.client and self.sub):
                return
            desired = {t["name"]: t for t in tags}
            # Unsubscribe removed
            for name in list(self.handles.keys()):
                if name not in desired:
                    h = self.handles.pop(name, None)
                    nid = next((k for k, v in self.nodeid_to_name.items() if v == name), None)
                    if nid:
                        self.nodeid_to_name.pop(nid, None)
                    if h is not None:
                        try: await self.sub.unsubscribe(h)
                        except Exception as e: LOG.warning("unsubscribe %s: %s", name, e)
            # Subscribe added
            for name, t in desired.items():
                if name in self.handles:
                    continue
                try:
                    node = self.client.get_node(t["node"])
                    h = await self.sub.subscribe_data_change(node)
                    self.handles[name] = h
                    self.nodeid_to_name[t["node"]] = name
                    LOG.info("subscribed %s (%s)", name, t["node"])
                except Exception as e:
                    LOG.warning("subscribe %s failed (%s)", name, e)

    async def browse_variables(self) -> list[dict]:
        """Walk Objects → collect Variable nodes. Capped at BROWSE_NODE_CAP."""
        if not (self.connected and self.client):
            raise RuntimeError("not connected")
        out: list[dict] = []
        seen: set[str] = set()
        root = self.client.nodes.objects
        await self._browse_recurse(root, out, seen, depth=0, max_depth=6, path="")
        return out[:BROWSE_NODE_CAP]

    async def _browse_recurse(self, node: Node, out: list, seen: set, depth: int, max_depth: int, path: str):
        if depth > max_depth or len(out) >= BROWSE_NODE_CAP:
            return
        try:
            children = await node.get_children()
        except Exception:
            return
        for child in children:
            try:
                nid = child.nodeid.to_string()
                if nid in seen:
                    continue
                seen.add(nid)
                # Skip the standard OPC UA Server metadata tree (ns=0); engineers
                # never want to log those.
                if child.nodeid.NamespaceIndex == 0:
                    continue
                bn = await child.read_browse_name()
                bn_str = bn.Name
                cls = await child.read_node_class()
                child_path = f"{path}/{bn_str}" if path else bn_str
                if cls == ua.NodeClass.Variable:
                    try:
                        dt_id = await child.read_data_type()
                        dt_name = _variant_type_name(dt_id)
                    except Exception:
                        dt_name = "?"
                    out.append({
                        "node": nid,
                        "name": bn_str,
                        "path": child_path,
                        "data_type": dt_name,
                    })
                    if len(out) >= BROWSE_NODE_CAP:
                        return
                elif cls in (ua.NodeClass.Object, ua.NodeClass.View):
                    await self._browse_recurse(child, out, seen, depth + 1, max_depth, child_path)
            except Exception:
                continue


# OPC UA builtin DataType NodeIds (ns=0). See OPC UA Part 3, §8.
_DATATYPE_NAMES = {
    1:  "Boolean",
    2:  "SByte",    3: "Byte",
    4:  "Int16",    5: "UInt16",
    6:  "Int32",    7: "UInt32",
    8:  "Int64",    9: "UInt64",
    10: "Float",   11: "Double",
    12: "String",  13: "DateTime",
    14: "Guid",    15: "ByteString",
    16: "XmlElement", 17: "NodeId", 18: "ExpandedNodeId",
    19: "StatusCode", 20: "QualifiedName", 21: "LocalizedText",
    22: "Structure",  23: "DataValue", 24: "BaseDataType", 25: "DiagnosticInfo",
    26: "Number",     27: "Integer",   28: "UInteger", 29: "Enumeration",
}

def _variant_type_name(dt_nodeid) -> str:
    """Best-effort NodeId → human name. Good enough for a picker UI."""
    try:
        ident = getattr(dt_nodeid, "Identifier", None)
        ns = getattr(dt_nodeid, "NamespaceIndex", None)
        if ns == 0 and isinstance(ident, int) and ident in _DATATYPE_NAMES:
            return _DATATYPE_NAMES[ident]
    except Exception:
        pass
    return "?"


# ── Writer + fan-out ──────────────────────────────────────────────────────

async def writer_task(app: web.Application):
    con: sqlite3.Connection = app["db"]
    q: asyncio.Queue = app["ingest_q"]
    sse_subs: list[asyncio.Queue] = app["sse_subs"]
    recent: deque = app["recent"]

    BATCH, FLUSH_INTERVAL = 100, 1.0
    buf: list[tuple] = []
    last_flush = time.monotonic()

    async def flush():
        nonlocal buf, last_flush
        if not buf: return
        with con:
            con.executemany(
                "INSERT INTO samples(ts, tag, value, quality) VALUES (?,?,?,?)",
                buf,
            )
        buf = []
        last_flush = time.monotonic()

    try:
        while True:
            try:
                s = await asyncio.wait_for(q.get(), timeout=FLUSH_INTERVAL)
            except asyncio.TimeoutError:
                await flush(); continue

            buf.append((s["ts"], s["id"], s["value"], s["q"]))
            recent.append(s)

            for sub in list(sse_subs):
                try:
                    sub.put_nowait(s)
                except asyncio.QueueFull:
                    try: sub.get_nowait()
                    except asyncio.QueueEmpty: pass
                    try: sub.put_nowait(s)
                    except asyncio.QueueFull: pass

            if len(buf) >= BATCH or (time.monotonic() - last_flush) >= FLUSH_INTERVAL:
                await flush()
    except asyncio.CancelledError:
        await flush(); raise


# ── Hot reload ────────────────────────────────────────────────────────────

async def tags_watcher_task(app: web.Application):
    last_mtime = TAGS_PATH.stat().st_mtime if TAGS_PATH.exists() else 0.0
    while True:
        try:
            await asyncio.sleep(TAG_WATCH_SEC)
            if not TAGS_PATH.exists(): continue
            mt = TAGS_PATH.stat().st_mtime
            if mt != last_mtime:
                last_mtime = mt
                try:
                    new_tags = load_tags()
                except Exception as e:
                    LOG.warning("tags.yaml invalid (%s) — ignoring", e)
                    continue
                app["tags"] = new_tags
                LOG.info("tags.yaml reloaded — %d tags", len(new_tags))
                mgr: ClientManager = app["mgr"]
                await mgr.reconcile(new_tags)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            LOG.warning("tags watcher error: %s", e)


# ── HTTP handlers ─────────────────────────────────────────────────────────

def _connection_payload(app: web.Application) -> dict:
    mgr: ClientManager = app["mgr"]
    conn = app["conn"]
    return {
        "endpoint": conn["endpoint"],
        "sample_interval_s": conn.get("sample_interval_s", 1.0),
        "connected": mgr.connected,
        "auth_mode": mgr.auth_mode,
        "security_policy": (conn.get("security") or {}).get("policy", "none"),
        "error": mgr.connect_error,
    }


async def handle_get_tags(request: web.Request):
    app = request.app
    out = []
    for t in app["tags"]:
        out.append({
            "id": t["name"],
            "node": t["node"],
            "category": t["category"],
            "unit": t["unit"],
            "min": t.get("min", 0.0),
            "max": t.get("max", 100.0),
            "avg": t.get("avg"),
            "group": t.get("group"),
        })
    payload = _connection_payload(app)
    payload["tags"] = out
    return web.json_response(payload)


async def handle_post_tag(request: web.Request):
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid JSON"}, status=400)
    try:
        tag = tag_from_payload(body)
    except ValueError as e:
        return web.json_response({"error": str(e)}, status=400)

    app = request.app
    tags = list(app["tags"])
    if any(t["name"] == tag["name"] for t in tags):
        return web.json_response({"error": f"tag '{tag['name']}' already exists"}, status=409)
    tags.append(tag)
    try:
        save_tags(tags)
    except Exception as e:
        return web.json_response({"error": f"failed to save: {e}"}, status=500)
    # Reconcile immediately (the watcher would pick it up in ~2s anyway)
    app["tags"] = tags
    mgr: ClientManager = app["mgr"]
    await mgr.reconcile(tags)
    return web.json_response({"ok": True, "tag": tag})


async def handle_delete_tag(request: web.Request):
    name = request.match_info["name"]
    app = request.app
    tags = [t for t in app["tags"] if t["name"] != name]
    if len(tags) == len(app["tags"]):
        return web.json_response({"error": f"tag '{name}' not found"}, status=404)
    try:
        save_tags(tags)
    except Exception as e:
        return web.json_response({"error": f"failed to save: {e}"}, status=500)
    app["tags"] = tags
    mgr: ClientManager = app["mgr"]
    await mgr.reconcile(tags)
    return web.json_response({"ok": True, "deleted": name})


async def handle_browse(request: web.Request):
    mgr: ClientManager = request.app["mgr"]
    if not mgr.connected:
        return web.json_response({"error": "OPC UA server not connected", "nodes": []}, status=503)
    try:
        nodes = await asyncio.wait_for(mgr.browse_variables(), timeout=10.0)
    except asyncio.TimeoutError:
        return web.json_response({"error": "browse timed out", "nodes": []}, status=504)
    except Exception as e:
        return web.json_response({"error": str(e), "nodes": []}, status=500)
    # Mark nodes already registered as tags
    known = {t["node"] for t in request.app["tags"]}
    for n in nodes:
        n["registered"] = n["node"] in known
    return web.json_response({"nodes": nodes, "capped": len(nodes) >= BROWSE_NODE_CAP})


async def handle_history(request: web.Request):
    con: sqlite3.Connection = request.app["db"]
    tags_param = request.query.get("tags", "")
    since_param = request.query.get("since")
    until_param = request.query.get("until")
    limit = int(request.query.get("limit", 5000))
    names = [t for t in tags_param.split(",") if t]
    if not names:
        return web.json_response({"error": "tags= required"}, status=400)
    since = _parse_time(since_param) if since_param else time.time() - 300
    until = _parse_time(until_param) if until_param else time.time() + 1
    cur = con.cursor()
    result = []
    for name in names:
        cur.execute(
            "SELECT ts, value FROM samples WHERE tag=? AND ts>=? AND ts<=? "
            "ORDER BY ts ASC LIMIT ?",
            (name, since, until, limit),
        )
        rows = cur.fetchall()
        result.append({"id": name, "points": [[r[0], r[1]] for r in rows]})
    return web.json_response({"since": since, "until": until, "series": result})


def _parse_time(s: str) -> float:
    try:
        return float(s)
    except ValueError:
        from datetime import datetime
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


async def handle_stream(request: web.Request):
    resp = web.StreamResponse(headers={
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    })
    await resp.prepare(request)
    q: asyncio.Queue = asyncio.Queue(maxsize=SSE_QUEUE_MAX)
    request.app["sse_subs"].append(q)
    await resp.write(b": connected\n\n")
    for s in list(request.app["recent"])[-60:]:
        await resp.write(f"data: {json.dumps(s)}\n\n".encode())
    try:
        while True:
            try:
                s = await asyncio.wait_for(q.get(), timeout=15.0)
                await resp.write(f"data: {json.dumps(s)}\n\n".encode())
            except asyncio.TimeoutError:
                await resp.write(b": keepalive\n\n")
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    finally:
        try: request.app["sse_subs"].remove(q)
        except ValueError: pass
    return resp


# ── Lifecycle ─────────────────────────────────────────────────────────────

async def on_startup(app: web.Application):
    app["conn"] = yaml.safe_load(CONN_PATH.read_text())
    app["tags"] = load_tags()
    app["db"] = init_db(DB_PATH)
    app["ingest_q"] = asyncio.Queue(maxsize=10_000)
    app["sse_subs"] = []
    app["recent"] = deque(maxlen=300)

    mgr = ClientManager(app["conn"], app)
    app["mgr"] = mgr
    app["bg_ingest"] = asyncio.create_task(mgr.run())
    app["bg_writer"] = asyncio.create_task(writer_task(app))
    app["bg_watcher"] = asyncio.create_task(tags_watcher_task(app))


async def on_cleanup(app: web.Application):
    for key in ("bg_ingest", "bg_writer", "bg_watcher"):
        t = app.get(key)
        if t:
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await t
    db = app.get("db")
    if db:
        db.close()


def make_app() -> web.Application:
    app = web.Application()
    app.router.add_get ("/live/tags",          handle_get_tags)
    app.router.add_post("/live/tags",          handle_post_tag)
    app.router.add_delete("/live/tags/{name}", handle_delete_tag)
    app.router.add_get ("/live/browse",        handle_browse)
    app.router.add_get ("/live/history",       handle_history)
    app.router.add_get ("/live/stream",        handle_stream)
    app.router.add_static("/", path=str(HERE), show_index=True)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    web.run_app(make_app(), host="127.0.0.1", port=8766)
