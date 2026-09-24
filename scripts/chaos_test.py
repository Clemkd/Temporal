#!/usr/bin/env python3
"""
End-to-end resilience test.

 1. starts 3 instances of the app (api + watcher, 2 workers) with transient fault injection;
 2. drops N files (valid / invalid / poison / slow) into incoming/;
 3. while they are processed: kills instances with SIGKILL and restarts them, optionally restarts
    the Temporal server container, starts a processing job and kills workers during it too;
 4. waits for the system to drain and checks the invariants:
      - every file left incoming/ and processing/ (all consumed)
      - processed/ + invalid/ == number of generated files
      - DB file statuses match the bucket, every expected-invalid file is invalid
      - measurements == sum(stored rows of Stored files): no loss, no duplicate
      - no measurement of an invalid file
      - processing job completed, every measurement categorized.

Usage: scripts/chaos_test.py [--files 1000] [--rows 100] [--kill-every 12] [--restart-temporal]
Requires: docker compose infra up (postgres, seaweedfs, temporal), app built in Release.
"""
import argparse, json, os, random, signal, subprocess, sys, time, urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(ROOT, "src/TemporalPoc.Api/bin/Release/net10.0")
LOGS = os.path.join(ROOT, "data/logs")
API = "http://localhost:5055"
PREFIX = ""

INSTANCES = {
    "api":     {"port": 5055, "env": {}},
    "worker1": {"port": 5056, "env": {"Watcher__AutoStart": "false"}},
    "worker2": {"port": 5057, "env": {"Watcher__AutoStart": "false"}},
}
procs = {}


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def http(method, path, body=None, base=API, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method, headers={"content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        return json.loads(raw) if raw else None


def psql(sql):
    out = subprocess.run(["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "poc", "-d", "temporal_poc", "-At", "-c", sql],
                         cwd=ROOT, capture_output=True, text=True, check=True)
    return out.stdout.strip()


def start(name, chaos_rate):
    cfg = INSTANCES[name]
    env = dict(os.environ, ASPNETCORE_URLS=f"http://localhost:{cfg['port']}",
               Chaos__TransientFailureRate=str(chaos_rate), Chaos__SlowSeconds="25", **cfg["env"])
    logf = open(os.path.join(LOGS, f"chaos-{name}.log"), "a")
    procs[name] = subprocess.Popen(["dotnet", "TemporalPoc.Api.dll"], cwd=BIN, env=env, stdout=logf, stderr=subprocess.STDOUT,
                                   start_new_session=True)


def kill(name):
    p = procs.get(name)
    if p and p.poll() is None:
        os.killpg(p.pid, signal.SIGKILL)
        p.wait()


def wait_healthy(port, timeout=90):
    end = time.time() + timeout
    while time.time() < end:
        try:
            http("GET", "/health", base=f"http://localhost:{port}", timeout=2)
            return True
        except Exception:
            time.sleep(1)
    return False


def stats():
    try:
        return http("GET", f"/api/stats?filePrefix={PREFIX}/", timeout=20)
    except Exception as e:
        return None


def show(s, prefix=""):
    if not s:
        log(prefix + "stats unavailable")
        return
    o, tq = s["objects"], s["taskQueues"]
    backlog = tq.get("file-ingestion/Activity", {}).get("backlog", "?")
    log(f"{prefix}incoming={o['incoming/']:5} processing={o['processing/']:4} processed={o['processed/']:5} invalid={o['invalid/']:4} "
        f"| running wf={s['workflows'].get('FileIngestion.running')} | ingestion activity backlog={backlog}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", type=int, default=1000)
    ap.add_argument("--rows", type=int, default=100)
    ap.add_argument("--kill-every", type=float, default=12)
    ap.add_argument("--chaos-rate", type=float, default=0.05)
    ap.add_argument("--restart-temporal", action="store_true")
    ap.add_argument("--timeout", type=int, default=1800)
    args = ap.parse_args()
    os.makedirs(LOGS, exist_ok=True)

    for name in INSTANCES:
        start(name, args.chaos_rate)
    for name, cfg in INSTANCES.items():
        if not wait_healthy(cfg["port"]):
            sys.exit(f"{name} did not start, see {LOGS}/chaos-{name}.log")
    log("3 instances up")

    # Default pipelines (a previous demo may have left a custom version as latest)
    for name in ("file-ingestion", "sensor-processing"):
        http("POST", f"/api/pipelines/{name}/reset")

    # Every run uses its own prefix: checks are restricted to it.
    global PREFIX
    prefix = PREFIX = f"chaos-{int(time.time())}"
    gen = http("POST", "/api/simulation/files", {"count": args.files, "rowsPerFile": args.rows, "invalidRatio": 0.05,
                                                 "poisonRatio": 0.01, "slowRatio": 0.01, "jsonRatio": 0.3, "prefix": prefix, "seed": 42},
               timeout=300)
    expected_invalid = gen["kinds"].get("Invalid", 0) + gen["kinds"].get("Poison", 0)
    log(f"generated {gen['generated']} files under {prefix}/: {gen['kinds']}")

    kills, temporal_restarts, job_started = 0, 0, False
    start_time = time.time()
    next_kill = time.time() + args.kill_every
    temporal_restart_at = time.time() + 45 if args.restart_temporal else None

    while True:
        time.sleep(3)
        # "restart policy": dead instances are restarted, like docker/k8s would do
        for name, p in procs.items():
            if p.poll() is not None:
                log(f"  {name} is down (exit {p.returncode}) -> restarting")
                start(name, args.chaos_rate)

        if time.time() >= next_kill:
            victim = random.choice(list(INSTANCES))
            log(f"  SIGKILL {victim}")
            kill(victim)
            kills += 1
            next_kill = time.time() + args.kill_every * random.uniform(0.6, 1.4)

        if temporal_restart_at and time.time() >= temporal_restart_at:
            log("  restarting the Temporal server container")
            subprocess.run(["docker", "compose", "restart", "temporal"], cwd=ROOT, capture_output=True)
            temporal_restarts += 1
            temporal_restart_at = None

        s = stats()
        show(s)
        if not s:
            continue
        o = s["objects"]
        if not job_started and o["processed/"] > args.files // 3:
            try:
                http("POST", "/api/processing/jobs", {"jobId": prefix, "batchSize": 250, "batchesPerRun": 10, "onlyUncategorized": False})
                job_started = True
                log("  processing job started (all sensor types) while files are still ingesting")
            except Exception as e:
                log(f"  could not start job yet: {e}")
        if o["incoming/"] == 0 and o["processing/"] == 0 and s["workflows"].get("FileIngestion.running") == 0:
            break
        if time.time() - start_time > args.timeout:
            sys.exit("TIMEOUT: system did not drain")

    elapsed = time.time() - start_time
    log(f"ingestion drained in {elapsed:.0f}s with {kills} kills and {temporal_restarts} Temporal restarts "
        f"({args.files / elapsed * 3600:.0f} files/hour)")

    # The processing jobs started mid-ingestion processed what existed at that time: run a final pass
    # (no chaos) to categorize the rest, then wait for all jobs.
    for name in INSTANCES:
        if procs[name].poll() is not None:
            start(name, 0)
    wait_healthy(5055)
    http("POST", "/api/processing/jobs", {"jobId": prefix + "-final", "batchSize": 1000})
    while True:
        jobs = http("GET", "/api/processing/jobs")
        mine = [j for j in jobs if j["jobId"].startswith(prefix)]
        if mine and all(j["status"] != "Running" for j in mine):
            break
        time.sleep(3)

    log("checking invariants")
    s = stats()
    show(s, "final: ")
    failures = []

    def check(cond, msg):
        log(("  OK   " if cond else "  FAIL ") + msg)
        if not cond:
            failures.append(msg)

    o = s["objects"]
    check(o["incoming/"] == 0 and o["processing/"] == 0, "all files consumed (incoming/ and processing/ empty)")
    check(o["processed/"] + o["invalid/"] == args.files, f"processed/ ({o['processed/']}) + invalid/ ({o['invalid/']}) == generated ({args.files})")
    check(o["invalid/"] == expected_invalid, f"invalid files ({o['invalid/']}) == generated invalid+poison ({expected_invalid})")
    mine = f"\"Key\" LIKE '{prefix}/%'"
    mine_m = f"\"FileKey\" LIKE '{prefix}/%'"
    db_stored = int(psql(f"SELECT count(*) FROM files WHERE {mine} AND \"Status\"='Stored'"))
    db_invalid = int(psql(f"SELECT count(*) FROM files WHERE {mine} AND \"Status\"='Invalid'"))
    false_invalid = psql(f"SELECT string_agg(\"Key\", ', ') FROM files WHERE {mine} AND \"Status\"='Invalid' "
                         f"AND \"Key\" NOT LIKE '%poison%' AND \"Error\" NOT LIKE 'Invalid%'")
    check(false_invalid == "", f"no file quarantined because of technical errors only ({false_invalid or 'none'})")
    check(db_stored == o["processed/"] and db_invalid == o["invalid/"], f"database statuses match the bucket (stored={db_stored}, invalid={db_invalid})")
    poison_ok = psql(f"SELECT count(*) FROM files WHERE {mine} AND \"Key\" LIKE '%poison%' AND \"Status\"<>'Invalid'")
    check(poison_ok == "0", "every poison file is quarantined")
    rows = int(psql(f"SELECT count(*) FROM measurements WHERE {mine_m}"))
    expected_rows = int(psql(f"SELECT coalesce(sum(\"StoredCount\"),0) FROM files WHERE {mine} AND \"Status\"='Stored'"))
    check(rows == expected_rows == db_stored * args.rows, f"measurements ({rows}) == stored rows ({expected_rows}) == files x rows ({db_stored * args.rows})")
    dup = psql(f"SELECT count(*) FROM (SELECT 1 FROM measurements WHERE {mine_m} GROUP BY \"FileKey\",\"LineNumber\" HAVING count(*)>1) d")
    check(dup == "0", "no duplicated measurement")
    orphan = psql(f"SELECT count(*) FROM measurements m JOIN files f ON f.\"Key\"=m.\"FileKey\" WHERE m.{mine_m} AND f.\"Status\"<>'Stored'")
    check(orphan == "0", "no measurement for an invalid file (compensation)")
    uncategorized = psql(f"SELECT count(*) FROM measurements WHERE {mine_m} AND \"Category\" IS NULL")
    check(uncategorized == "0", "every measurement categorized")
    failed_jobs = psql(f"SELECT count(*) FROM processing_jobs WHERE \"JobId\" LIKE '{prefix}%' AND \"Status\"<>'Completed'")
    check(failed_jobs == "0", "all processing jobs completed")
    staging = psql(f"SELECT count(*) FROM categorization_staging WHERE \"JobId\" LIKE '{prefix}%'")
    check(staging == "0", "staging table cleaned")

    for name in list(INSTANCES):
        kill(name)
    log("RESULT: " + ("SUCCESS" if not failures else f"{len(failures)} FAILURE(S)"))
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
