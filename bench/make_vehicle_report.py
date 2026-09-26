#!/usr/bin/env python3
"""Builds bench/report-vehicles/index.html from bench/results-vehicles/<label>/."""
import json, math, os, re
from make_report import read_csv, birth, bucketize, parse_db_after

ROOT = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(ROOT, "results-vehicles")
ORDER = ["fanout", "w10", "w1", "period7", "actw10"]
NAMES = {"fanout": "Enfants : 365 d'un coup", "w10": "Enfants : 10 à la fois", "w1": "Enfants : 1 à la fois", "actw10": "Activités : 10 à la fois", "period7": "Enfants en séquence, période de 7 j"}
METRICS = ["temporal_cpu_pct", "temporal_mem_mb", "postgres_cpu_pct", "postgres_mem_mb", "worker_cpu_pct", "worker_mem_mb",
           "insert_per_s", "insert_mean_ms", "commits_per_s", "db_size_mb"]
MAX_POINTS = 500


def load(label):
    d = os.path.join(RESULTS, label)
    if not all(os.path.exists(os.path.join(d, f)) for f in ("bench.csv", "metrics.csv")):
        return None
    bench, metrics = read_csv(os.path.join(d, "bench.csv")), read_csv(os.path.join(d, "metrics.csv"))
    for r in metrics:
        # The worker process ends with the run: its last CPU delta is meaningless.
        if r["worker_mem_mb"] <= 0 or r["worker_cpu_pct"] < 0:
            r["worker_cpu_pct"] = 0
    if not bench or not metrics:
        return None
    t0 = birth(os.path.join(d, "bench.csv"))
    sp = os.path.join(d, "bench.summary.json")
    summary = json.load(open(sp)) if os.path.exists(sp) else None
    end = summary["elapsed_s"] if summary else bench[-1]["elapsed_s"]
    bucket = max(1, math.ceil((end + 10) / MAX_POINTS))
    mpoints = [(r["ts"] - t0, r) for r in metrics if -5 <= r["ts"] - t0 <= end + 10]
    bpoints = [(r["elapsed_s"], r) for r in bench]
    mb = bucketize(mpoints, bucket, METRICS)
    bb = bucketize(bpoints, bucket, ["days_per_s", "days_done", "vehicles_done"])
    import bisect
    be, bd = [r["elapsed_s"] for r in bench], [r["days_done"] for r in bench]

    def days_at(t):
        i = bisect.bisect_left(be, t)
        if t <= 0 or not be:
            return 0
        if i >= len(be):
            return bd[-1]
        if i == 0:
            return bd[0] * t / be[0]
        return bd[i - 1] + (bd[i] - bd[i - 1]) * (t - be[i - 1]) / (be[i] - be[i - 1])

    series = {"t": [round(t / 60, 3) for t, _ in mb], "started": [round(days_at(t)) for t, _ in mb]}
    for k in METRICS:
        series[k] = [round(r[k], 3) for _, r in mb]
    bseries = {"t": [round(t / 60, 3) for t, _ in bb], "started": [round(days_at(t)) for t, _ in bb]}
    for k in ["days_per_s", "days_done", "vehicles_done"]:
        bseries[k] = [round(r[k], 3) for _, r in bb]

    load_rows = [r for t, r in mpoints if 0 <= t <= end]
    days_done = bench[-1]["days_done"] if not summary else summary["days_done"]
    total_ins = sum(r["insert_per_s"] for r in load_rows)
    db = parse_db_after(os.path.join(d, "db_after.txt"))
    size = db["db_size_mb"] or metrics[-1]["db_size_mb"]
    vehicles = summary["vehicles"] if summary else 1500
    days = summary["days"] if summary else 365
    rate = days_done / end if end else 0
    avg = lambda k: round(sum(r[k] for r in load_rows) / max(1, len(load_rows)), 1)
    mx = lambda k: round(max((r[k] for r in load_rows), default=0))
    stats = {
        "label": label, "name": NAMES[label], "done": summary is not None,
        "vehicles": vehicles, "days": days, "total": vehicles * days,
        "days_done": days_done, "vehicles_done": summary["vehicles_done"] if summary else int(bench[-1]["vehicles_done"]),
        "complete": bool(summary and summary.get("complete")),
        "elapsed_s": round(end, 1), "rate": round(rate, 1),
        "eta_full_s": round(1500 * 365 / rate) if rate else None,
        "errors": summary["errors"] if summary else int(bench[-1]["errors_total"]),
        "temporal_cpu_avg": avg("temporal_cpu_pct"), "postgres_cpu_avg": avg("postgres_cpu_pct"), "worker_cpu_avg": avg("worker_cpu_pct"),
        "temporal_mem_max": mx("temporal_mem_mb"), "postgres_mem_max": mx("postgres_mem_mb"), "worker_mem_max": mx("worker_mem_mb"),
        "insert_mean_ms": round(sum(r["insert_per_s"] * r["insert_mean_ms"] for r in load_rows) / total_ins, 3) if total_ins else 0,
        "inserts_per_day": round(total_ins / days_done, 1) if days_done else 0,
        "db_size_mb": size,
        "kb_per_day": round((size - 18) * 1024 / days_done, 1) if days_done else None,
    }
    return {"stats": stats, "metrics": series, "bench": bseries, "db": db}


def main():
    data = {k: v for k, v in ((label, load(label)) for label in ORDER) if v}
    html = open(os.path.join(ROOT, "report_vehicles_template.html")).read()
    html = html.replace("/*__DATA__*/null", json.dumps(data, separators=(",", ":")))
    notes = os.path.join(ROOT, "report_vehicles_notes.html")
    if os.path.exists(notes):
        html = html.replace("<!--__NOTES__-->", open(notes).read())
    os.makedirs(os.path.join(ROOT, "report-vehicles"), exist_ok=True)
    out = os.path.join(ROOT, "report-vehicles", "index.html")
    open(out, "w").write(html)
    for v in data.values():
        s = v["stats"]
        print(f'{s["label"]:7} {int(s["days_done"]):>8,}/{s["total"]:,} days {s["rate"]:>6}/s veh_done {s["vehicles_done"]} '
              f'cpu T/P/W {s["temporal_cpu_avg"]}/{s["postgres_cpu_avg"]}/{s["worker_cpu_avg"]} mem T/P/W {s["temporal_mem_max"]}/{s["postgres_mem_max"]}/{s["worker_mem_max"]} '
              f'ins {s["insert_mean_ms"]}ms x{s["inserts_per_day"]} db {s["db_size_mb"]}MB {s["kb_per_day"]}KB/day')
    print("->", out)


if __name__ == "__main__":
    main()
