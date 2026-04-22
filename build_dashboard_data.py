"""
Process Data_log_10.rdb (SQLite) + Data_log_52410.csv into a compact JSON
bundle consumed by dashboard.html. Downsamples time series to keep the
payload small while preserving shape.
"""
import csv
import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

HERE = Path(__file__).parent
RDB = HERE / "Data_log_10.rdb"
CSV_FILE = HERE / "Data_log_52410.csv"
OUT = HERE / "dashboard_data.json"

OLE_EPOCH = datetime(1899, 12, 30)

# Max points per tag after downsampling (keeps bundle light, chart snappy).
MAX_POINTS = 1500


def ole_to_iso(time_ms: float) -> str:
    ole = time_ms / 1_000_000.0
    return (OLE_EPOCH + timedelta(days=ole)).strftime("%Y-%m-%dT%H:%M:%S")


def downsample(points, target):
    """LTTB-lite: keep first/last, bucket-average the middle. Preserves shape."""
    if len(points) <= target:
        return points
    n = len(points)
    bucket = n / (target - 2)
    out = [points[0]]
    for i in range(1, target - 1):
        lo = min(n - 1, int(round(i * bucket)))
        hi = min(n, int(round((i + 1) * bucket)))
        if hi <= lo:
            hi = lo + 1
        chunk = points[lo:hi]
        ts = chunk[len(chunk) // 2][0]
        v = sum(p[1] for p in chunk) / len(chunk)
        out.append([ts, round(v, 4)])
    out.append(points[-1])
    return out


def load_sqlite():
    con = sqlite3.connect(RDB)
    cur = con.cursor()
    cur.execute(
        "SELECT VarName, Time_ms, VarValue, Validity "
        "FROM logdata ORDER BY Time_ms"
    )
    series = defaultdict(list)
    validity_bad = defaultdict(int)
    for name, tms, val, valid in cur.fetchall():
        if val is None:
            continue
        series[name].append([ole_to_iso(tms), float(val)])
        if not valid:
            validity_bad[name] += 1
    con.close()

    # Drop tags that are entirely zero (the CS group) — not useful on the chart.
    meaningful = {}
    for name, pts in series.items():
        if not pts:
            continue
        vals = [p[1] for p in pts]
        if max(vals) == 0 and min(vals) == 0:
            continue
        if name.startswith("$"):
            continue
        meaningful[name] = {
            "points": downsample(pts, MAX_POINTS),
            "raw_count": len(pts),
            "min": round(min(vals), 4),
            "max": round(max(vals), 4),
            "avg": round(sum(vals) / len(vals), 4),
            "bad_samples": validity_bad[name],
            "first": pts[0][0],
            "last": pts[-1][0],
        }
    return meaningful


def load_csv():
    series = defaultdict(list)
    validity_bad = defaultdict(int)
    with open(CSV_FILE, newline="") as fh:
        reader = csv.reader(fh)
        next(reader)  # header
        for row in reader:
            if len(row) < 5:
                continue
            name, ts, val, valid = row[0], row[1], row[2], row[3]
            if name.startswith("$") or name.startswith("Consigne"):
                continue
            try:
                v = float(val)
            except ValueError:
                continue
            series[name].append([ts.replace(" ", "T"), v])
            if valid != "1":
                validity_bad[name] += 1

    out = {}
    for name, pts in series.items():
        if not pts:
            continue
        vals = [p[1] for p in pts]
        out[name] = {
            "points": downsample(pts, MAX_POINTS),
            "raw_count": len(pts),
            "min": round(min(vals), 4),
            "max": round(max(vals), 4),
            "avg": round(sum(vals) / len(vals), 4),
            "bad_samples": validity_bad[name],
            "first": pts[0][0],
            "last": pts[-1][0],
        }
    return out


def categorize(name: str) -> str:
    n = name.upper()
    if "TE" in n and "RATE" not in n:
        return "temperature"
    if "_T1" in n or "_T2" in n:
        return "temperature"
    if "PT" in n:
        return "pressure"
    if "VB" in n:
        return "vibration"
    if "LT" in n or "LEVEL" in n or "FILL" in n:
        return "level"
    if "PID" in n or "CV" in n or "CHILLER" in n:
        return "control"
    if "SPEED" in n:
        return "speed"
    if "CURRENT" in n:
        return "current"
    return "other"


def unit_for(name: str) -> str:
    c = categorize(name)
    return {
        "temperature": "°C",
        "pressure": "bar",
        "vibration": "mm/s",
        "level": "mm",
        "control": "%",
        "speed": "rpm",
        "current": "A",
    }.get(c, "")


def friendly(name: str) -> str:
    # Shorten long Siemens tag names for display.
    short = name
    short = short.replace("DB_", "").replace("FB_", "")
    short = short.replace("_akt_Out_real", "")
    short = short.replace("_Act_", " ")
    short = short.replace("_Param_Out_Output", " OUT")
    short = short.replace("MCC01-LP01_5241_", "")
    short = short.replace("MCC01_5241-", "")
    return short


def build():
    rdb_series = load_sqlite()
    csv_series = load_csv()

    def pack(series_dict, source):
        tags = []
        for name, data in series_dict.items():
            tags.append({
                "id": name,
                "label": friendly(name),
                "source": source,
                "category": categorize(name),
                "unit": unit_for(name),
                "stats": {
                    "min": data["min"],
                    "max": data["max"],
                    "avg": data["avg"],
                    "count": data["raw_count"],
                    "bad": data["bad_samples"],
                    "first": data["first"],
                    "last": data["last"],
                },
                "points": data["points"],
            })
        return tags

    bundle = {
        "generated": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "sources": [
            {
                "name": "Data_log_10.rdb",
                "kind": "SQLite 3 (WinCC archive)",
                "tags": pack(rdb_series, "process"),
            },
            {
                "name": "Data_log_52410.csv",
                "kind": "CSV (WinCC RT)",
                "tags": pack(csv_series, "drive_5241"),
            },
        ],
    }

    OUT.write_text(json.dumps(bundle, separators=(",", ":")))
    kb = OUT.stat().st_size / 1024
    total_tags = sum(len(s["tags"]) for s in bundle["sources"])
    print(f"wrote {OUT.name} — {kb:.1f} KB, {total_tags} tags")


if __name__ == "__main__":
    build()
