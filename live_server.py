"""
Live ingest + HTTP server in one process.

  PLC ──[OPC UA subscribe | S7 poll]──▶ Backend ──▶ SQLite (samples, WAL)
                                            │
                                            └─▶ fan-out queue ──▶ SSE subs

Config
  connection.yaml — `backend: opcua | s7`, plus per-backend settings.
                    PLC_USER/PLC_PASS env vars override OPC UA auth.
  tags.yaml       — list of tags; hot-reloaded on mtime change, so edits
                    on disk OR via the dashboard's "Add tag" button take
                    effect without a process restart.

HTTP endpoints
  GET    /live/tags          → tag catalog + connection status (incl. categories)
  POST   /live/tags          → add a new tag (schema depends on backend)
  DELETE /live/tags/{name}   → remove a tag
  GET    /live/categories    → list of user categories (custom + in-use)
  POST   /live/categories    → add a custom category (`{"name": "..."}`)
  DELETE /live/categories/{name} → remove a category (refuses if a tag uses it)
  GET    /live/browse        → walk PLC address space (OPC UA only; 501 on s7)
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
BROWSE_NODE_CAP = 500    # safety cap when walking the OPC UA address space


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
#
# A tag is a dict with backend-agnostic fields (name, category, unit,
# min/max) plus addressing fields specific to one backend:
#   OPC UA: node     (e.g. "ns=2;s=Process.TE1")
#   S7:     area, offset, type, optional db, optional bit
# A tag may carry both sets of fields (the inactive backend just ignores
# the irrelevant ones), so the same tags.yaml file can be reused if you
# switch backends later.

_S7_TYPE_SIZES = {
    "bool":  1,    # one byte, plus a bit index
    "byte":  1,
    "int":   2,
    "word":  2,
    "dint":  4,
    "dword": 4,
    "real":  4,
}

_S7_AREAS = {"db", "m", "mk", "i", "pe", "input", "q", "pa", "output"}


def _is_valid_opcua_tag(t: dict) -> bool:
    return bool(t.get("node"))


def _is_valid_s7_tag(t: dict) -> bool:
    area = (t.get("area") or "").lower()
    if area not in _S7_AREAS:
        return False
    if "offset" not in t:
        return False
    if (t.get("type") or "").lower() not in _S7_TYPE_SIZES:
        return False
    if area == "db" and not t.get("db"):
        return False
    return True


def load_tags(backend: str) -> list[dict]:
    """
    Read tags.yaml and return tags valid for the active backend.
    Tags missing the active backend's addressing fields are skipped with
    a warning, not raised, so a single tags.yaml can survive a backend
    switch without a manual rewrite.
    """
    raw = yaml.safe_load(TAGS_PATH.read_text()) or {}
    tags = raw.get("tags", []) or []
    out: list[dict] = []
    by_name: dict[str, dict] = {}
    for t in tags:
        if "name" not in t:
            raise ValueError(f"tag missing name: {t}")
        t.setdefault("category", "other")
        t.setdefault("unit", "")
        if backend == "opcua" and not _is_valid_opcua_tag(t):
            LOG.warning("skipping tag %r: no OPC UA 'node' field", t["name"])
            continue
        if backend == "s7" and not _is_valid_s7_tag(t):
            LOG.warning("skipping tag %r: missing S7 addressing (area/offset/type[/db])",
                        t["name"])
            continue
        if t["name"] in by_name:
            raise ValueError(f"duplicate tag name: {t['name']}")
        by_name[t["name"]] = t
        out.append(t)
    return out


def load_categories() -> list[str]:
    """User-defined categories from tags.yaml. May contain entries no tag
    uses yet (pre-registered for upcoming tags)."""
    raw = yaml.safe_load(TAGS_PATH.read_text()) or {}
    cats = raw.get("categories") or []
    seen, out = set(), []
    for c in cats:
        s = str(c).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def merged_categories(tags: list[dict], custom: list[str]) -> list[str]:
    """Categories actually in use on tags, plus user-registered customs."""
    seen, out = set(), []
    for t in tags:
        c = (t.get("category") or "other").strip()
        if c and c not in seen:
            seen.add(c); out.append(c)
    for c in custom:
        if c not in seen:
            seen.add(c); out.append(c)
    return sorted(out)


def save_config(tags: list[dict], categories: list[str]):
    """Atomic write to tags.yaml. Preserves header comment."""
    header = (
        "# Tag list. Managed by live_server.py (edits from the dashboard\n"
        "# land here too). Connection settings live in connection.yaml.\n"
        "#\n"
        "# OPC UA tags carry: node\n"
        "# S7 tags carry:    area, offset, type, optional db, optional bit\n"
        "# A tag may carry both for cross-backend reuse.\n"
        "# `categories` holds custom categories the dashboard should offer\n"
        "# even when no tag is currently using them.\n\n"
    )
    body: dict = {}
    if categories:
        body["categories"] = list(categories)
    body["tags"] = tags
    tmp = TAGS_PATH.with_suffix(".yaml.tmp")
    tmp.write_text(header + yaml.safe_dump(body, sort_keys=False, allow_unicode=True))
    tmp.replace(TAGS_PATH)


# Back-compat shim: existing callers pass tags only.
def save_tags(tags: list[dict]):
    save_config(tags, load_categories())


def tag_from_payload(body: dict, backend: str) -> dict:
    """Validate and normalize an incoming tag POST."""
    name = str(body.get("name", "")).strip()
    if not name:
        raise ValueError("name must not be empty")
    out: dict = {
        "name":     name,
        "category": (str(body.get("category", "")).strip() or "other"),
        "unit":     str(body.get("unit", "")).strip(),
        "min":      float(body.get("min", 0.0)),
        "max":      float(body.get("max", 100.0)),
    }
    if out["max"] <= out["min"]:
        out["max"] = out["min"] + 1.0

    if backend == "opcua":
        node = str(body.get("node", "")).strip()
        if not node:
            raise ValueError("node must not be empty for OPC UA backend")
        out["node"] = node
    elif backend == "s7":
        area = str(body.get("area", "")).strip().lower()
        if area not in _S7_AREAS:
            raise ValueError(f"area must be one of {sorted(_S7_AREAS)}")
        ttype = str(body.get("type", "")).strip().lower()
        if ttype not in _S7_TYPE_SIZES:
            raise ValueError(f"type must be one of {sorted(_S7_TYPE_SIZES)}")
        try:
            offset = int(body.get("offset"))
        except (TypeError, ValueError):
            raise ValueError("offset must be an integer (bytes)")
        out["area"] = area
        out["offset"] = offset
        out["type"] = ttype
        if area == "db":
            try:
                out["db"] = int(body["db"])
            except (KeyError, TypeError, ValueError):
                raise ValueError("db must be an integer when area=db")
        if ttype == "bool":
            try:
                out["bit"] = int(body.get("bit", 0))
            except (TypeError, ValueError):
                raise ValueError("bit must be an integer 0..7")
            if not 0 <= out["bit"] <= 7:
                raise ValueError("bit must be in 0..7")
    else:
        raise ValueError(f"unknown backend: {backend}")
    return out


# ── OPC UA backend ────────────────────────────────────────────────────────

class _OpcuaSubHandler:
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


class OpcuaBackend:
    """
    OPC UA backend. Owns the asyncua Client, the subscription, and the
    per-tag handle map. .reconcile(tags) syncs the subscription against
    the desired tag set. Retries the underlying connection on any failure.
    """
    name = "opcua"

    def __init__(self, conn: dict, app: web.Application):
        self.conn = conn
        self.app = app
        self.client: Optional[Client] = None
        self.sub = None
        self.handles: dict[str, Any] = {}         # name -> subscription handle
        self.nodeid_to_name: dict[str, str] = {}  # nodeid string -> tag name
        self.connected = False
        self.connect_error: Optional[str] = None
        self.auth_mode = "anonymous"
        self._reconcile_lock = asyncio.Lock()

    @property
    def endpoint_label(self) -> str:
        return self.conn.get("endpoint", "")

    @property
    def transport_label(self) -> str:
        return self.auth_mode

    def _apply_auth(self, client: Client):
        """Env vars > connection.yaml. Anonymous when neither is set."""
        user = os.environ.get("PLC_USER") or (self.conn.get("auth") or {}).get("username")
        pwd  = os.environ.get("PLC_PASS") or (self.conn.get("auth") or {}).get("password")
        cfg_mode = (self.conn.get("auth") or {}).get("mode", "anonymous").lower()
        if user and pwd:
            client.set_user(user)
            client.set_password(pwd)
            self.auth_mode = "password"
        elif cfg_mode == "password":
            LOG.warning("auth mode=password but PLC_USER/PLC_PASS not set, falling back to anonymous")
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
        policy_name = policy.replace("_", "").title() if policy != "basic256sha256" else "Basic256Sha256"
        await client.set_security_string(f"{policy_name},SignAndEncrypt,{cert},{key}")

    async def run(self):
        endpoint = self.conn["endpoint"]
        interval_ms = int(float(self.conn.get("sample_interval_s", 1.0)) * 1000)

        while True:
            try:
                LOG.info("opcua: connecting to %s", endpoint)
                self.connect_error = None
                client = Client(endpoint)
                self._apply_auth(client)
                await self._apply_security(client)
                self.client = client
                async with client:
                    handler = _OpcuaSubHandler(asyncio.get_running_loop(),
                                               self.app["ingest_q"],
                                               self.nodeid_to_name)
                    self.sub = await client.create_subscription(interval_ms, handler)
                    self.connected = True
                    LOG.info("opcua: connected (%s auth)", self.auth_mode)
                    await self.reconcile(self.app["tags"])
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
                LOG.warning("opcua: connection failed (%s), retrying in 5s", e)
                await asyncio.sleep(5.0)

    async def reconcile(self, tags: list[dict]):
        """Sync subscription state to match `tags`. Safe to call concurrently."""
        async with self._reconcile_lock:
            if not (self.connected and self.client and self.sub):
                return
            desired = {t["name"]: t for t in tags if _is_valid_opcua_tag(t)}
            for name in list(self.handles.keys()):
                if name not in desired:
                    h = self.handles.pop(name, None)
                    nid = next((k for k, v in self.nodeid_to_name.items() if v == name), None)
                    if nid:
                        self.nodeid_to_name.pop(nid, None)
                    if h is not None:
                        try: await self.sub.unsubscribe(h)
                        except Exception as e: LOG.warning("unsubscribe %s: %s", name, e)
            for name, t in desired.items():
                if name in self.handles:
                    continue
                try:
                    node = self.client.get_node(t["node"])
                    h = await self.sub.subscribe_data_change(node)
                    self.handles[name] = h
                    self.nodeid_to_name[t["node"]] = name
                    LOG.info("opcua: subscribed %s (%s)", name, t["node"])
                except Exception as e:
                    LOG.warning("opcua: subscribe %s failed (%s)", name, e)

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
    try:
        ident = getattr(dt_nodeid, "Identifier", None)
        ns = getattr(dt_nodeid, "NamespaceIndex", None)
        if ns == 0 and isinstance(ident, int) and ident in _DATATYPE_NAMES:
            return _DATATYPE_NAMES[ident]
    except Exception:
        pass
    return "?"


# ── S7 backend (snap7) ────────────────────────────────────────────────────

try:
    import snap7
    from snap7.util import (
        get_bool, get_int, get_dint, get_real, get_word, get_dword,
    )
    try:
        from snap7.type import Area as _SNAP7_AREAS  # snap7 >= 2.0
    except ImportError:
        from snap7.types import Areas as _SNAP7_AREAS  # snap7 1.x
    _SNAP7_OK = True
    _SNAP7_IMPORT_ERROR: Optional[str] = None
except Exception as _e:
    _SNAP7_OK = False
    _SNAP7_IMPORT_ERROR = str(_e)


def _s7_decode(buf: bytes, offset_in_buf: int, t: str, bit: int = 0) -> float:
    t = t.lower()
    if t == "bool":  return float(get_bool(buf, offset_in_buf, bit))
    if t == "byte":  return float(buf[offset_in_buf])
    if t == "int":   return float(get_int(buf, offset_in_buf))
    if t == "word":  return float(get_word(buf, offset_in_buf))
    if t == "dint":  return float(get_dint(buf, offset_in_buf))
    if t == "dword": return float(get_dword(buf, offset_in_buf))
    if t == "real":  return float(get_real(buf, offset_in_buf))
    raise ValueError(f"unknown S7 type: {t}")


def _s7_area_const(area: str):
    a = area.lower()
    if a in ("m", "mk"):
        return _SNAP7_AREAS.MK
    if a in ("i", "pe", "input"):
        return _SNAP7_AREAS.PE
    if a in ("q", "pa", "output"):
        return _SNAP7_AREAS.PA
    raise ValueError(f"unsupported area: {area}")


class S7Backend:
    """
    Native S7 protocol backend over python-snap7. Polls all configured
    tags every `sample_interval_s`. snap7 is synchronous, so each PLC
    round-trip runs in the default thread executor to keep the asyncio
    loop responsive.

    PLC-side prerequisites (TIA Portal, S7-1500 / ET200SP):
      * CPU → Properties → Protection & Security → "Permit access with
        PUT/GET communication from remote partner" must be ticked.
      * Every DB this backend reads must have "Optimized block access"
        DISABLED (DB → Properties → Attributes), otherwise byte offsets
        are not stable.
    """
    name = "s7"

    def __init__(self, conn: dict, app: web.Application):
        if not _SNAP7_OK:
            raise RuntimeError(
                "python-snap7 not available: " + (_SNAP7_IMPORT_ERROR or "?") +
                ". Install with `pip install python-snap7`; on macOS also `brew install snap7`."
            )
        self.conn = conn
        self.app = app
        s7conf = (conn.get("s7") or {})
        self.host = str(s7conf.get("host") or "")
        self.rack = int(s7conf.get("rack", 0))
        self.slot = int(s7conf.get("slot", 1))
        self.interval = float(conn.get("sample_interval_s", 1.0))
        self.connected = False
        self.connect_error: Optional[str] = None
        self.auth_mode = "s7-comm (PUT/GET)"
        self._client = None
        self._desired: list[dict] = []
        self._desired_lock = asyncio.Lock()

    @property
    def endpoint_label(self) -> str:
        return f"s7://{self.host}:102 (rack {self.rack}, slot {self.slot})"

    @property
    def transport_label(self) -> str:
        return self.auth_mode

    async def run(self):
        if not self.host:
            self.connect_error = "s7.host not set in connection.yaml"
            LOG.error("s7: %s", self.connect_error)
            return

        loop = asyncio.get_running_loop()
        while True:
            self.connect_error = None
            try:
                self._client = snap7.client.Client()
                LOG.info("s7: connecting to %s rack=%d slot=%d", self.host, self.rack, self.slot)
                await loop.run_in_executor(None, self._connect_blocking)
                self.connected = True
                LOG.info("s7: connected")
                await self.reconcile(self.app["tags"])
                while True:
                    await self._poll_once(loop)
                    await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.connected = False
                self.connect_error = str(e)
                LOG.warning("s7: error (%s), retrying in 5s", e)
                with contextlib.suppress(Exception):
                    if self._client is not None:
                        self._client.disconnect()
                self._client = None
                await asyncio.sleep(5.0)

    def _connect_blocking(self):
        self._client.connect(self.host, self.rack, self.slot)

    async def _poll_once(self, loop):
        async with self._desired_lock:
            tags = list(self._desired)
        if not tags:
            return
        samples = await loop.run_in_executor(None, self._read_all_blocking, tags)
        ts = time.time()
        q: asyncio.Queue = self.app["ingest_q"]
        for name, value, q_ok in samples:
            sample = {"id": name, "ts": ts, "value": float(value), "q": 1 if q_ok else 0}
            try:
                q.put_nowait(sample)
            except asyncio.QueueFull:
                try: q.get_nowait()
                except asyncio.QueueEmpty: pass
                with contextlib.suppress(asyncio.QueueFull):
                    q.put_nowait(sample)

    def _read_all_blocking(self, tags: list[dict]) -> list[tuple]:
        """Group tags by (area, db); one round trip per group; decode in place."""
        groups: dict[tuple, list[dict]] = {}
        for t in tags:
            area = t["area"].lower()
            db = int(t.get("db") or 0)
            groups.setdefault((area, db), []).append(t)
        out: list[tuple] = []
        for (area, db), gtags in groups.items():
            offsets = [(int(t["offset"]), _S7_TYPE_SIZES[t["type"].lower()]) for t in gtags]
            start = min(o for o, _ in offsets)
            end = max(o + s for o, s in offsets)
            length = end - start
            try:
                buf = self._read_area_blocking(area, db, start, length)
            except Exception as e:
                LOG.warning("s7: read %s/db%d @%d+%d failed: %s", area, db, start, length, e)
                for t in gtags:
                    out.append((t["name"], 0.0, False))
                continue
            for t in gtags:
                rel = int(t["offset"]) - start
                try:
                    val = _s7_decode(buf, rel, t["type"], int(t.get("bit", 0)))
                    out.append((t["name"], val, True))
                except Exception as e:
                    LOG.warning("s7: decode %s failed: %s", t["name"], e)
                    out.append((t["name"], 0.0, False))
        return out

    def _read_area_blocking(self, area: str, db: int, start: int, length: int) -> bytes:
        if length <= 0:
            return b""
        a = area.lower()
        if a == "db":
            return bytes(self._client.db_read(db, start, length))
        return bytes(self._client.read_area(_s7_area_const(a), 0, start, length))

    async def reconcile(self, tags: list[dict]):
        valid = [t for t in tags if _is_valid_s7_tag(t)]
        async with self._desired_lock:
            self._desired = valid
        LOG.info("s7: %d tags configured for polling", len(valid))

    async def browse_variables(self) -> list[dict]:
        raise RuntimeError(
            "Browse not supported on the S7 backend. Add tags manually with "
            "area + db + offset + type."
        )


# ── Backend factory ───────────────────────────────────────────────────────

def make_backend(conn: dict, app: web.Application):
    backend = (conn.get("backend") or "opcua").lower()
    if backend == "opcua":
        return OpcuaBackend(conn, app)
    if backend == "s7":
        return S7Backend(conn, app)
    raise ValueError(f"unknown backend in connection.yaml: {backend!r}")


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
                    new_tags = load_tags(app["backend_name"])
                    new_cats = load_categories()
                except Exception as e:
                    LOG.warning("tags.yaml invalid (%s), ignoring", e)
                    continue
                app["tags"] = new_tags
                app["categories"] = new_cats
                LOG.info("tags.yaml reloaded, %d tags, %d custom categories",
                         len(new_tags), len(new_cats))
                mgr = app["mgr"]
                await mgr.reconcile(new_tags)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            LOG.warning("tags watcher error: %s", e)


# ── HTTP handlers ─────────────────────────────────────────────────────────

def _connection_payload(app: web.Application) -> dict:
    mgr = app["mgr"]
    conn = app["conn"]
    return {
        "backend": app["backend_name"],
        "endpoint": mgr.endpoint_label,
        "sample_interval_s": conn.get("sample_interval_s", 1.0),
        "connected": mgr.connected,
        "auth_mode": mgr.auth_mode,
        "security_policy": (conn.get("security") or {}).get("policy", "none"),
        "error": mgr.connect_error,
        "browse_supported": app["backend_name"] == "opcua",
    }


def _tag_to_wire(t: dict) -> dict:
    """Project a stored tag into the JSON shape the dashboard expects."""
    out = {
        "id": t["name"],
        "category": t.get("category", "other"),
        "unit": t.get("unit", ""),
        "min": t.get("min", 0.0),
        "max": t.get("max", 100.0),
        "avg": t.get("avg"),
        "group": t.get("group"),
    }
    if "node" in t:
        out["node"] = t["node"]
    for f in ("area", "db", "offset", "type", "bit"):
        if f in t:
            out[f] = t[f]
    return out


async def handle_get_tags(request: web.Request):
    app = request.app
    out = [_tag_to_wire(t) for t in app["tags"]]
    payload = _connection_payload(app)
    payload["tags"] = out
    payload["categories"] = merged_categories(app["tags"], app["categories"])
    return web.json_response(payload)


async def handle_get_categories(request: web.Request):
    app = request.app
    return web.json_response({
        "categories": merged_categories(app["tags"], app["categories"]),
        "custom": list(app["categories"]),
    })


async def handle_post_category(request: web.Request):
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid JSON"}, status=400)
    name = str(body.get("name", "")).strip()
    if not name:
        return web.json_response({"error": "name must not be empty"}, status=400)
    if len(name) > 64 or any(c in name for c in "\n\r\t"):
        return web.json_response({"error": "invalid category name"}, status=400)
    app = request.app
    existing = merged_categories(app["tags"], app["categories"])
    if name in existing:
        return web.json_response({"error": f"category '{name}' already exists"}, status=409)
    new_custom = list(app["categories"]) + [name]
    try:
        save_config(list(app["tags"]), new_custom)
    except Exception as e:
        return web.json_response({"error": f"failed to save: {e}"}, status=500)
    app["categories"] = new_custom
    return web.json_response({
        "ok": True,
        "categories": merged_categories(app["tags"], new_custom),
    })


async def handle_delete_category(request: web.Request):
    name = request.match_info["name"]
    app = request.app
    in_use = [t["name"] for t in app["tags"] if (t.get("category") or "other") == name]
    if in_use:
        return web.json_response(
            {"error": f"category '{name}' is in use by {len(in_use)} tag(s)",
             "tags": in_use},
            status=409,
        )
    if name not in app["categories"]:
        return web.json_response({"error": f"category '{name}' is not removable"}, status=404)
    new_custom = [c for c in app["categories"] if c != name]
    try:
        save_config(list(app["tags"]), new_custom)
    except Exception as e:
        return web.json_response({"error": f"failed to save: {e}"}, status=500)
    app["categories"] = new_custom
    return web.json_response({
        "ok": True, "deleted": name,
        "categories": merged_categories(app["tags"], new_custom),
    })


async def handle_post_tag(request: web.Request):
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid JSON"}, status=400)
    app = request.app
    try:
        tag = tag_from_payload(body, app["backend_name"])
    except ValueError as e:
        return web.json_response({"error": str(e)}, status=400)

    tags = list(app["tags"])
    if any(t["name"] == tag["name"] for t in tags):
        return web.json_response({"error": f"tag '{tag['name']}' already exists"}, status=409)
    tags.append(tag)
    try:
        save_tags(tags)
    except Exception as e:
        return web.json_response({"error": f"failed to save: {e}"}, status=500)
    app["tags"] = tags
    mgr = app["mgr"]
    await mgr.reconcile(tags)
    return web.json_response({"ok": True, "tag": _tag_to_wire(tag)})


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
    mgr = app["mgr"]
    await mgr.reconcile(tags)
    return web.json_response({"ok": True, "deleted": name})


async def handle_browse(request: web.Request):
    app = request.app
    if app["backend_name"] != "opcua":
        return web.json_response(
            {"error": "Browse is only available on the OPC UA backend.", "nodes": []},
            status=501,
        )
    mgr: OpcuaBackend = app["mgr"]
    if not mgr.connected:
        return web.json_response({"error": "OPC UA server not connected", "nodes": []}, status=503)
    try:
        nodes = await asyncio.wait_for(mgr.browse_variables(), timeout=10.0)
    except asyncio.TimeoutError:
        return web.json_response({"error": "browse timed out", "nodes": []}, status=504)
    except Exception as e:
        return web.json_response({"error": str(e), "nodes": []}, status=500)
    known = {t["node"] for t in app["tags"] if "node" in t}
    for n in nodes:
        n["registered"] = n["node"] in known
    return web.json_response({"nodes": nodes, "capped": len(nodes) >= BROWSE_NODE_CAP})


async def handle_range(request: web.Request):
    """Min/max sample timestamps across the whole archive. The dashboard's
    Archive mode uses this to anchor its date pickers and the 'All' button."""
    con: sqlite3.Connection = request.app["db"]
    cur = con.cursor()
    cur.execute("SELECT MIN(ts), MAX(ts) FROM samples")
    row = cur.fetchone()
    if not row or row[0] is None:
        return web.json_response({"first": None, "last": None, "empty": True})
    return web.json_response({"first": float(row[0]), "last": float(row[1]), "empty": False})


async def handle_history(request: web.Request):
    con: sqlite3.Connection = request.app["db"]
    tags_param = request.query.get("tags", "")
    since_param = request.query.get("since")
    until_param = request.query.get("until")
    # 100k points covers ~28h at 1 Hz per tag, plenty for the dashboard's
    # default 24h archive view. ECharts LTTB downsamples on the client.
    limit = int(request.query.get("limit", 100_000))
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
    app["backend_name"] = (app["conn"].get("backend") or "opcua").lower()
    app["tags"] = load_tags(app["backend_name"])
    app["categories"] = load_categories()
    app["db"] = init_db(DB_PATH)
    app["ingest_q"] = asyncio.Queue(maxsize=10_000)
    app["sse_subs"] = []
    app["recent"] = deque(maxlen=300)

    mgr = make_backend(app["conn"], app)
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
    app.router.add_get ("/live/categories",          handle_get_categories)
    app.router.add_post("/live/categories",          handle_post_category)
    app.router.add_delete("/live/categories/{name}", handle_delete_category)
    app.router.add_get ("/live/browse",        handle_browse)
    app.router.add_get ("/live/range",         handle_range)
    app.router.add_get ("/live/history",       handle_history)
    app.router.add_get ("/live/stream",        handle_stream)
    app.router.add_static("/", path=str(HERE), show_index=True)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    web.run_app(make_app(), host="127.0.0.1", port=8766)
