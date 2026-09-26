#!/usr/bin/env python3
"""Builds bench/report/index.html from bench/results/<label>/ (bench.csv, metrics.csv, summary, db_after.txt)."""
import csv, json, math, os, re, subprocess, bisect

ROOT = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(ROOT, "results")
ORDER = ["seq", "b100", "b1000", "b10000"]
NAMES = {"seq": "1 par 1", "b100": "Paquets de 100", "b1000": "Paquets de 1 000", "b10000": "Paquets de 10 000"}
METRICS = ["temporal_cpu_pct", "temporal_mem_mb", "postgres_cpu_pct", "postgres_mem_mb", "insert_per_s",
           "insert_mean_ms", "update_mean_ms", "commits_per_s", "rows_inserted_per_s", "db_size_mb"]
MAX_POINTS = 500


def read_csv(path):
    with open(path) as f:
        return [{k: float(v) for k, v in row.items()} for row in csv.DictReader(f)]


def birth(path):
    out = subprocess.run(["stat", "-c", "%W", path], capture_output=True, text=True).stdout.strip()
    return int(out) if out and out != "0" else int(os.stat(path).st_mtime)


def bucketize(points, bucket, keys):
    """points: list of (t, row). Returns averaged buckets [(t_center, {key: mean})]."""
    out, cur, start = [], [], None
    for t, row in points:
        b = math.floor(t / bucket)
        if start is not None and b != start:
            out.append(((start + 0.5) * bucket, {k: sum(r[k] for r in cur) / len(cur) for k in keys}))
            cur = []
        start = b
        cur.append(row)
    if cur:
        out.append(((start + 0.5) * bucket, {k: sum(r[k] for r in cur) / len(cur) for k in keys}))
    return out


def parse_db_after(path):
    tables, statements, size = [], [], None
    if not os.path.exists(path):
        return {"tables": tables, "statements": statements, "db_size_mb": size}
    for line in open(path):
        parts = line.rstrip("\n").split("|")
        if parts[0] == "db_size_mb":
            size = float(parts[1])
        elif len(parts) == 3 and re.match(r"^[a-z_]+$", parts[0]) and parts[1].isdigit():
            tables.append({"table": parts[0], "rows": int(parts[1]), "size": parts[2]})
        elif len(parts) == 3 and re.match(r"^[0-9.]+$", parts[0]):
            statements.append({"mean_ms": float(parts[0]), "calls": int(parts[1]), "query": parts[2]})
    return {"tables": tables, "statements": statements, "db_size_mb": size}


def load(label):
    d = os.path.join(RESULTS, label)
    if not os.path.exists(os.path.join(d, "bench.csv")) or not os.path.exists(os.path.join(d, "metrics.csv")):
        return None
    bench = read_csv(os.path.join(d, "bench.csv"))
    metrics = read_csv(os.path.join(d, "metrics.csv"))
    if not bench or not metrics:
        return None
    t0 = birth(os.path.join(d, "bench.csv"))
    summary_path = os.path.join(d, "bench.summary.json")
    summary = json.load(open(summary_path)) if os.path.exists(summary_path) else None
    done = summary is not None
    count_target = summary["count"] if summary else None
    b_elapsed = [r["elapsed_s"] for r in bench]
    b_started = [r["started_total"] for r in bench]
    end = summary["elapsed_s"] if summary else b_elapsed[-1]

    def started_at(t):
        if t <= 0:
            return 0
        i = bisect.bisect_left(b_elapsed, t)
        if i >= len(b_elapsed):
            return b_started[-1]
        if i == 0:
            return b_started[0] * t / b_elapsed[0]
        t1, t2, s1, s2 = b_elapsed[i - 1], b_elapsed[i], b_started[i - 1], b_started[i]
        return s1 + (s2 - s1) * (t - t1) / (t2 - t1)

    duration = end + 20
    bucket = max(1, math.ceil(duration / MAX_POINTS))
    mpoints = [(r["ts"] - t0, r) for r in metrics if -5 <= r["ts"] - t0 <= end + 20]
    bpoints = [(r["elapsed_s"], r) for r in bench]
    mb = bucketize(mpoints, bucket, METRICS)
    bb = bucketize(bpoints, bucket, ["starts_per_s", "lat_p50_ms", "lat_p99_ms", "lat_max_ms"])

    series = {"t": [round(t / 60, 3) for t, _ in mb], "started": [round(started_at(t)) for t, _ in mb]}
    for k in METRICS:
        series[k] = [round(r[k], 3) for _, r in mb]
    bseries = {"t": [round(t / 60, 3) for t, _ in bb], "started": [round(started_at(t)) for t, _ in bb]}
    for k in ["starts_per_s", "lat_p50_ms", "lat_p99_ms", "lat_max_ms"]:
        bseries[k] = [round(r[k], 3) for _, r in bb]

    load_rows = [r for t, r in mpoints if 0 <= t <= end]
    total_ins = sum(r["insert_per_s"] for r in load_rows)
    weighted = sum(r["starts_per_s"] * r["lat_p50_ms"] for r in bench)
    total_started = b_started[-1]
    dbinfo = parse_db_after(os.path.join(d, "db_after.txt"))
    final_size = dbinfo["db_size_mb"] or (metrics[-1]["db_size_mb"] if metrics else None)
    stats = {
        "label": label, "name": NAMES[label], "done": done,
        "count": total_started, "target": count_target,
        "elapsed_s": round(end, 1), "rate": round(total_started / end, 1) if end else 0,
        "errors": summary["errors"] if summary else int(bench[-1]["errors_total"]),
        "p50_ms": round(weighted / max(1, sum(r["starts_per_s"] for r in bench)), 1),
        "p99_ms_max": round(max(r["lat_p99_ms"] for r in bench), 1),
        "temporal_cpu_avg": round(sum(r["temporal_cpu_pct"] for r in load_rows) / max(1, len(load_rows)), 1),
        "postgres_cpu_avg": round(sum(r["postgres_cpu_pct"] for r in load_rows) / max(1, len(load_rows)), 1),
        "temporal_mem_max": round(max((r["temporal_mem_mb"] for r in load_rows), default=0)),
        "postgres_mem_max": round(max((r["postgres_mem_mb"] for r in load_rows), default=0)),
        "insert_mean_ms": round(sum(r["insert_per_s"] * r["insert_mean_ms"] for r in load_rows) / total_ins, 3) if total_ins else 0,
        "inserts_per_start": round(total_ins / total_started, 1) if total_started else 0,
        "db_size_mb": final_size,
        "kb_per_start": round((final_size - 18) * 1024 / total_started, 1) if final_size and total_started else None,
    }
    return {"stats": stats, "metrics": series, "bench": bseries, "db": dbinfo}


def main():
    data = {label: load(label) for label in ORDER}
    data = {k: v for k, v in data.items() if v}
    template = open(os.path.join(ROOT, "report_template.html")).read()
    html = template.replace("/*__DATA__*/null", json.dumps(data, separators=(",", ":")))
    out = os.path.join(ROOT, "report", "index.html")
    open(out, "w").write(html)
    for v in data.values():
        s = v["stats"]
        print(f'{s["label"]:7} {s["count"]:>9,} starts  {s["rate"]:>7}/s  p50 {s["p50_ms"]} ms  pg cpu {s["postgres_cpu_avg"]}%  '
              f'insert {s["insert_mean_ms"]} ms  db {s["db_size_mb"]} MB  done={s["done"]}')
    print("->", out, f"{os.path.getsize(out) // 1024} KB")


if __name__ == "__main__":
    main()
