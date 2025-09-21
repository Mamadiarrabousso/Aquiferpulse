# app/main.py — clean, stable API
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
from collections import Counter
import json, csv


app = FastAPI(title="AquiferPulse")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# ----- Paths (anchored to project root) -----
ROOT          = Path(__file__).resolve().parents[1]
DATA          = ROOT / "data"
PROCESSED_DIR = DATA / "processed"
STATIC_DIR    = DATA / "static"

ASI_LATEST = PROCESSED_DIR / "asi_latest.geojson"
ASI_TABLE  = PROCESSED_DIR / "asi_table.csv"
BASINS     = STATIC_DIR / "basins.geojson"

# ----- Helpers -----
def _read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))

def _r(x, nd=3):
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip().lower() in {"", "none", "nan"}:
            return None
        return round(float(x), nd)
    except Exception:
        return None

# ----- Core builders -----
def latest_geojson():
    """Return the latest FeatureCollection, falling back to basins with 'no-data'."""
    # Primary: prebuilt file from compute_asi.py
    if ASI_LATEST.exists():
        try:
            return _read_json(ASI_LATEST)
        except Exception:
            pass

    # Fallback: basins with no-data props
    gj = _read_json(BASINS)
    for f in gj.get("features", []):
        p = f.setdefault("properties", {})
        p.update({"date": None, "twsa_z": None, "sm_z": None,
                  "rain_z": None, "rain_def_z": None, "asi": None, "class": "no-data"})
    return gj

def geojson_for_date(date: str):
    """
    Build a FeatureCollection for a specific month (YYYY-MM or YYYY-MM-01)
    from basins.geojson + asi_table.csv.
    """
    if not ASI_TABLE.exists():
        raise HTTPException(404, detail="asi_table.csv not found. Run scripts/compute_asi.py")

    # normalize to first-of-month
    try:
        dt = datetime.strptime(date[:7] + "-01", "%Y-%m-%d")
        target = dt.strftime("%Y-%m-%d")
    except Exception:
        raise HTTPException(400, detail="date must be YYYY-MM or YYYY-MM-01")

    # read rows for that month only
    rows = []
    with ASI_TABLE.open("r", encoding="utf-8", newline="") as fh:
        rdr = csv.DictReader(fh)
        for r in rdr:
            if r.get("date") == target:
                rows.append(r)
    by_id = {str(r["basin_id"]): r for r in rows}

    gj = _read_json(BASINS)
    for f in gj.get("features", []):
        p = f.setdefault("properties", {})
        bid = str(p.get("basin_id"))
        r = by_id.get(bid)
        if r:
            p.update({
                "date": target,
                "twsa_z": _r(r.get("twsa_z")), "sm_z": _r(r.get("sm_z")),
                "rain_z": _r(r.get("rain_z")), "rain_def_z": _r(r.get("rain_def_z")),
                "asi": _r(r.get("asi")), "class": r.get("class"),
                "name": p.get("name") or bid,
            })
        else:
            p.update({"date": target, "twsa_z": None, "sm_z": None,
                      "rain_z": None, "rain_def_z": None, "asi": None, "class": "no-data"})
    return gj

# ----- Routes -----
@app.get("/")
def root():
    return {"service": "AquiferPulse", "status": "ok"}

@app.get("/health")
def health():
    def info(p: Path):
        return {"exists": p.exists(), "size": (p.stat().st_size if p.exists() else 0), "path": str(p)}
    feat_count = None
    if ASI_LATEST.exists():
        try: feat_count = len(_read_json(ASI_LATEST).get("features", []))
        except Exception: feat_count = -1
    return {
        "cwd": str(Path().resolve()),
        "root": str(ROOT),
        "processed_dir": str(PROCESSED_DIR),
        "static_dir": str(STATIC_DIR),
        "asi_latest": info(ASI_LATEST) | {"features": feat_count},
        "asi_table":  info(ASI_TABLE),
        "basins":     info(BASINS),
    }

@app.get("/asi/latest")
def asi_latest():
    return latest_geojson()

@app.get("/asi/at")
def asi_at(date: str):
    """GeoJSON for a given month (YYYY-MM or YYYY-MM-01)."""
    return geojson_for_date(date)

@app.get("/asi/top10")
def asi_top10(limit: int = 10, classes: str = "alert,watch", date: str | None = None):
    wanted = {c.strip() for c in classes.split(",") if c.strip()}
    gj = geojson_for_date(date) if date else latest_geojson()
    rows = []
    for f in gj.get("features", []):
        p = f.get("properties", {}) or {}
        asi = p.get("asi")
        if isinstance(asi, (int, float)):
            rows.append({
                "basin_id": p.get("basin_id") or f.get("id"),
                "name": p.get("name"),
                "asi": _r(asi),
                "class": p.get("class"),
                "date": p.get("date"),
            })
    if wanted:
        rows = [r for r in rows if r["class"] in wanted]
    rows.sort(key=lambda r: r["asi"])
    return rows[:max(0, int(limit))]

@app.get("/asi/summary")
def asi_summary(date: str | None = None):
    gj = geojson_for_date(date) if date else latest_geojson()
    classes = [(f.get("properties") or {}).get("class", "no-data") for f in gj.get("features", [])]
    counts = Counter(classes)
    as_of = next(((f.get("properties") or {}).get("date")
                  for f in gj.get("features", [])
                  if (f.get("properties") or {}).get("date")), None)
    asis = [ (f.get("properties") or {}).get("asi")
             for f in gj.get("features", [])
             if isinstance((f.get("properties") or {}).get("asi"), (int, float)) ]
    return {"as_of": as_of, "counts": counts, "min_asi": (min(asis) if asis else None), "max_asi": (max(asis) if asis else None)}
@app.get("/asi/latest_date")
def asi_latest_date():
    """Return the max date present in data/processed/asi_table.csv."""
    if not ASI_TABLE.exists():
        raise HTTPException(404, detail="asi_table.csv not found. Run scripts/compute_asi.py")
    latest = None
    with ASI_TABLE.open("r", encoding="utf-8", newline="") as fh:
        rdr = csv.DictReader(fh)
        for r in rdr:
            d = r.get("date")
            if d:
                latest = d if latest is None else (d if d > latest else latest)
    return {"latest": latest}
@app.get("/asi/date_range")
def asi_date_range():
    """Return min/max dates present in data/processed/asi_table.csv."""
    if not ASI_TABLE.exists():
        raise HTTPException(404, detail="asi_table.csv not found. Run scripts/compute_asi.py")
    dmin, dmax = None, None
    with ASI_TABLE.open("r", encoding="utf-8", newline="") as fh:
        rdr = csv.DictReader(fh)
        for r in rdr:
            d = (r.get("date") or "").strip()[:10]  # 'YYYY-MM-DD' or 'YYYY-MM-DD hh:mm:ss'
            if not d:
                continue
            dmin = d if dmin is None or d < dmin else dmin
            dmax = d if dmax is None or d > dmax else dmax
    return {"min": dmin, "max": dmax}


@app.get("/asi/history")
def asi_history(basin_id: str):
    if not ASI_TABLE.exists():
        raise HTTPException(404, detail="asi_table.csv not found. Run scripts/compute_asi.py")
    out = []
    with ASI_TABLE.open("r", encoding="utf-8", newline="") as fh:
        rdr = csv.DictReader(fh)
        for row in rdr:
            if str(row.get("basin_id")) == str(basin_id):
                out.append({
                    "date": row.get("date"),
                    "twsa_z": _r(row.get("twsa_z")), "sm_z": _r(row.get("sm_z")),
                    "rain_z": _r(row.get("rain_z")), "rain_def_z": _r(row.get("rain_def_z")),
                    "asi": _r(row.get("asi")), "class": row.get("class"),
                })
    if not out:
        raise HTTPException(404, detail=f"No history for basin_id={basin_id}")
    return out
# --- Backwards-compat so the existing HTML keeps working ---

@app.get("/api/asi")
def legacy_api_asi():
    # old URL -> latest GeoJSON
    return latest_geojson()

@app.get("/api/asi_at")
def legacy_api_asi_at(date: str):
    # old URL with ?date=YYYY-MM -> month GeoJSON
    return geojson_for_date(date)

@app.get("/api/summary")
def legacy_api_summary(date: str | None = None):
    return asi_summary(date)

@app.get("/api/top10")
def legacy_api_top10(limit: int = 10, classes: str = "alert,watch", date: str | None = None):
    return asi_top10(limit=limit, classes=classes, date=date)

