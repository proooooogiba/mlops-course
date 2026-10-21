"""Dataset rules and HTML reporting, deliberately independent of training."""
import html
import json
from pathlib import Path

import numpy as np
import pandas as pd

from pipeline.storage import atomic_bytes, atomic_json, db, root, run_token


def inspect_dataset(run_key):
    """Read a consistent warehouse snapshot; write counts and DQ rule results.

    A run-specific JSON result is replaced on retry. No source rows or pointers change.
    """
    with db() as conn:
        rows = conn.execute("SELECT transaction_id,transaction_time,payload,target FROM labeled_transactions").fetchall()
    records = [dict(json.loads(row["payload"]), transaction_id=row["transaction_id"],
                    transaction_time=row["transaction_time"], target=row["target"]) for row in rows]
    checks = []
    def check(name, condition, detail):
        checks.append({"rule": name, "passed": bool(condition), "detail": detail})
    check("Nonempty dataset", len(records) > 0, f"rows={len(records)}")
    frame = pd.DataFrame(records)
    required = {"transaction_id", "transaction_time", "amount", "population_city", "lat", "lon",
                "merchant_lat", "merchant_lon", "cat_id", "us_state", "gender", "merch", "jobs", "target"}
    schema_ok = required <= set(frame.columns)
    check("Required schema", schema_ok, str(sorted(required - set(frame.columns))))
    if schema_ok:
        check("Unique IDs", frame.transaction_id.is_unique, "transaction_id is a primary key")
        timestamps = pd.to_datetime(frame.transaction_time, errors="coerce", utc=True)
        check("Valid timestamps", timestamps.notna().all(), f"invalid={timestamps.isna().sum()}")
        check("Binary labels", frame.target.notna().all() and frame.target.isin([0, 1]).all(), "target in {0,1}")
        for name in ("amount", "population_city"):
            values = pd.to_numeric(frame[name], errors="coerce")
            check(f"Finite nonnegative {name}", (np.isfinite(values) & (values >= 0)).all(), f"min={values.min()}")
        geographic = True
        for name, limit in (("lat", 90), ("lon", 180), ("merchant_lat", 90), ("merchant_lon", 180)):
            values = pd.to_numeric(frame[name], errors="coerce")
            geographic &= bool((np.isfinite(values) & values.between(-limit, limit)).all())
        check("Coordinate bounds", geographic, "latitude ±90, longitude ±180")
        check("Nonempty categories", all((frame[name].notna() & frame[name].astype(str).str.strip().ne("")).all()
              for name in ("cat_id", "us_state", "gender", "merch", "jobs")), "five categorical features")
        months = timestamps.dt.strftime("%Y-%m")
        per_month = frame.groupby(months).target.nunique()
        check("Both classes each month", len(per_month) >= 3 and per_month.eq(2).all(), str(per_month.to_dict()))
        distinct = sorted(months.dropna().unique())
        from pipeline.storage import add_month
        consecutive = bool(distinct) and all(add_month(distinct[0], i) == m for i, m in enumerate(distinct))
        check("Consecutive months", consecutive, str(distinct))
    result = {"run_key": run_key, "rows": len(records), "checks": checks,
              "passed": all(c["passed"] for c in checks)}
    target = root() / "reports" / f"dq_{run_token(run_key)}.json"
    atomic_json(target, result)
    return str(target)


def render_report(result_path):
    """Read DQ JSON and publish escaped HTML, including failures, before task assertion.

    Atomic replacement prevents half-written reports; reruns replace only this report.
    """
    result = json.loads(Path(result_path).read_text())
    rows = "".join(f"<tr><td>{html.escape(c['rule'])}</td><td>{'PASS' if c['passed'] else 'FAIL'}</td><td>{html.escape(c['detail'])}</td></tr>" for c in result["checks"])
    document = f"""<!doctype html><html lang="en"><meta charset="utf-8"><title>Fraud Data Quality</title>
    <style>body{{font:16px system-ui;max-width:1100px;margin:40px auto}}table{{border-collapse:collapse;width:100%}}td,th{{border:1px solid #ddd;padding:10px;text-align:left}}</style>
    <h1>Fraud Data Quality: {'PASS' if result['passed'] else 'FAIL'}</h1>
    <p>Run: {html.escape(result['run_key'])}; rows: {result['rows']}</p>
    <table><tr><th>Rule</th><th>Result</th><th>Details</th></tr>{rows}</table></html>"""
    target = Path(result_path).with_suffix(".html")
    atomic_bytes(target, document.encode())
    atomic_bytes(root() / "reports/latest.html", document.encode())
    return result_path


def assert_quality(result_path):
    """Read the already published check result and fail the DAG if any rule failed.

    This read-only assertion can be retried; even failed runs retain their HTML report.
    """
    result = json.loads(Path(result_path).read_text())
    failures = [c["rule"] for c in result["checks"] if not c["passed"]]
    if failures:
        raise ValueError("DQ failed: " + ", ".join(failures))
