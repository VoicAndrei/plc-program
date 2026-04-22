# plc-program

Lightweight OPC UA ingest + dashboard for Siemens PLCs. Built to replace
WinCC's data logging (which caps out at 500k rows per file — about 20 hours
of plant data at typical sampling rates).

## What it does

- Subscribes to configured OPC UA tags on a PLC at 1 Hz (configurable).
- Writes every sample to SQLite (WAL mode, indexed). No practical row limit.
- Serves a browser dashboard (`report.html`) with:
  - Live mode: streaming chart, per-tag scrollback from SQLite, CSV/PNG export.
  - Tag management: **Add / Delete tags from the dashboard itself**, including
    a "Browse PLC" picker that walks the address space.
  - Archive mode (optional): opens legacy WinCC `.rdb` + `.csv` dumps when
    they're present in this folder.

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

## Authentication

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

Two ways — both take effect with no restart:

1. **In the dashboard** → click **+ Add tag** in live mode. The _Browse PLC_
   tab lists every Variable node the server exposes; click **Add →** on any
   row to pre-fill the form, confirm unit + category, hit _Save tag_.
2. **Edit `tags.yaml`** directly. The server watches the file and
   re-subscribes on any change.

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

- **BadIdentityTokenRejected** on connect → PLC has anonymous disabled.
  Either enable it in TIA Portal or use `PLC_USER`/`PLC_PASS` env vars.
- **Connection refused / timeout** → wrong `endpoint`, PLC firewall, or
  OPC UA server not started on the PLC side.
- **Dashboard shows "Disconnected"** → check the Live badge; the server
  retries every 5 s. Errors are surfaced in the terminal log.
- **Tags not appearing after edit to `tags.yaml`** → the watcher polls
  every 2 s; also check the terminal for "tags.yaml invalid" warnings
  (YAML syntax error).
