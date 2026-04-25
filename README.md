# plc-program

Lightweight ingest + dashboard for Siemens PLCs. Built to replace WinCC's
data logging, which caps out at 500k rows per file (about 20 hours of plant
data at typical sampling rates).

## What it does

- Reads configured PLC tags at 1 Hz (configurable). Two backends:
  - **OPC UA** (`asyncua`): subscription-based, requires an OPC UA license
    on the S7-1500.
  - **S7** (`python-snap7`): native S7 protocol over port 102, polling. No
    PLC-side license, but TIA needs PUT/GET enabled and DBs to be
    non-optimized.
- Writes every sample to SQLite (WAL mode, indexed). No practical row limit.
- Serves a browser dashboard (`report.html`) with:
  - Live mode: streaming chart, per-tag scrollback from SQLite, CSV/PNG export.
  - Tag management: **Add / Delete tags from the dashboard itself**. On OPC UA
    a "Browse PLC" picker walks the address space; on S7 you enter
    DB/offset/type by hand.
  - Archive mode (optional): opens legacy WinCC `.rdb` + `.csv` dumps when
    they're present in this folder.

## Picking a backend

Open `connection.yaml` and set `backend:` to either `opcua` or `s7`. Use
the rest of that file's per-backend block to configure. Tag definitions in
`tags.yaml` are filtered by addressing fields, so an OPC UA tag list and
an S7 tag list can coexist if you ever switch.

## Setup on a new laptop

### Windows
1. Install Python 3.10 or newer from [python.org](https://www.python.org/downloads/).
   **Important:** tick _"Add Python to PATH"_ in the installer.
2. Double-click `setup.bat`. It creates a `.venv` and installs dependencies.
3. Open `connection.yaml` in Notepad and set `endpoint` to your PLC's URL
   (e.g. `opc.tcp://192.168.0.10:4840/...` — check TIA Portal → OPC UA).
4. Double-click `run.bat`.
5. Open [http://localhost:8766/report.html?mode=live](http://localhost:8766/report.html?mode=live).

### macOS / Linux
```bash
./setup.sh          # creates .venv, installs deps
# edit connection.yaml
./run.sh
```

## S7 backend (snap7)

Pick this when the PLC has no OPC UA license, or when you want to skip
OPC UA configuration entirely. It talks the native S7 protocol on port 102.

**TIA Portal prerequisites for S7-1500 / ET200SP:**
1. CPU → Properties → **Protection & Security** → tick "Permit access
   with PUT/GET communication from remote partner".
2. Every DB this dashboard reads must have **Optimized block access
   disabled** (DB → Properties → Attributes). Otherwise byte offsets are
   not stable from outside the program.
3. Compile + download.

**Rack/slot tip:** ET200SP-CPU (1510SP, 1512SP, 1515SP) usually wants
`rack=0, slot=1`. If `connect()` errors out, try `slot=0`. Standalone
S7-1500 = slot 1, S7-300/400 = slot 2.

**Snap7 C library install (only the S7 backend needs it):**
- Windows: bundled by recent `python-snap7` wheels, no action needed.
- macOS:   `brew install snap7`
- Linux:   `apt-get install libsnap7-1` or build from sourceforge.

**Tag schema for S7 in `tags.yaml`:**
```yaml
tags:
  - name: motor_speed
    area: db          # db | m | i | q
    db: 10            # required when area=db
    offset: 4         # byte offset into the area
    type: real        # bool | byte | int | word | dint | dword | real
    unit: rpm
    category: speed
    min: 0
    max: 1500
  - name: estop_active
    area: m
    offset: 0
    type: bool
    bit: 3            # required when type=bool
    unit: ""
    category: control
```

Add tags from the dashboard's **+ Add tag** modal too: when `backend: s7`,
the modal swaps the NodeId field for area/DB/offset/type/bit inputs.

## Authentication (OPC UA backend)

The PLC network is usually physically isolated, so **anonymous access**
is the default.

If your Siemens S7-1500 has anonymous disabled (TIA Portal → OPC UA →
Server → Security → Permit anonymous user authentication), set
credentials via env vars before running:

**Windows:**
```
set PLC_USER=youruser
set PLC_PASS=yourpass
run.bat
```

**macOS/Linux:**
```bash
export PLC_USER=youruser
export PLC_PASS=yourpass
./run.sh
```

Credentials are never stored in any file in this repo.

## Adding tags

Two ways, both take effect with no restart:

1. **In the dashboard** → click **+ Add tag** in live mode.
   - On **OPC UA**: the _Browse PLC_ tab lists every Variable node the
     server exposes; click **Add →** on any row to pre-fill the form,
     confirm unit + category, hit _Save tag_.
   - On **S7**: there is no remote browse, so the modal opens straight on
     _Manual entry_ and asks for area/DB/offset/type.
2. **Edit `tags.yaml`** directly. The server watches the file and
   re-subscribes (or re-polls) on any change.

Delete tags via the × button on each inventory row (SQLite history is
kept; only the live subscription stops).

## File layout

```
connection.yaml    PLC endpoint, auth mode, security policy
tags.yaml          Tag list (also managed from the UI)
live_server.py     Main app: OPC UA subscribe + SQLite write + HTTP + SSE
opcua_server.py    Local sim server (only needed when no real PLC is available)
report.html        Dashboard — Archive and Live modes
data/plc.db        SQLite archive (ignored by git; created on first run)
```

## Running against the bundled simulator

Useful for dashboard work on a laptop without a PLC:

```
python opcua_server.py         # terminal 1 — simulated 13-tag PLC
python live_server.py          # terminal 2 — ingest + dashboard
```

Open the dashboard URL above; values will be the sim's realistic waveforms.

## Offline install (optional)

If the target laptop can't reach PyPI, pre-download wheels on a machine
that can:

```
pip download -r requirements.txt -d wheels
```

Copy the `wheels/` folder into the project directory on the target.
`setup.bat` detects it automatically and installs offline.

## Troubleshooting

OPC UA backend:
- **BadIdentityTokenRejected** on connect → PLC has anonymous disabled.
  Either enable it in TIA Portal or use `PLC_USER`/`PLC_PASS` env vars.
- **Connection refused / timeout** → wrong `endpoint`, PLC firewall, or
  OPC UA server not started on the PLC side.
- **OPC UA license not sufficient** at compile time → in TIA, set CPU →
  OPC UA → Server → Runtime license to "OPC UA small" (or higher) and
  activate the license via Automation License Manager.

S7 backend:
- **`Connection refused` / `TCP:Unreachable peer`** → PUT/GET access not
  enabled on the CPU, or wrong rack/slot. Try `slot=0` for ET200SP-CPU.
- **`No data received`** when reading a DB → the DB has Optimized block
  access enabled. Disable it in DB → Properties → Attributes, then
  download.
- **`OSError: snap7 library not found`** → install the C library: macOS
  `brew install snap7`; Linux `apt-get install libsnap7-1`; Windows
  upgrade `python-snap7` to a wheel that bundles `snap7.dll`.

Both:
- **Dashboard shows "Disconnected"** → check the Live badge; the server
  retries every 5 s. Errors are surfaced in the terminal log.
- **Tags not appearing after edit to `tags.yaml`** → the watcher polls
  every 2 s; also check the terminal for "tags.yaml invalid" warnings
  (YAML syntax error). Tags missing the active backend's addressing
  fields are skipped with a warning.
