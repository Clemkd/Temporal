#!/usr/bin/env python3
"""
Growing task queue scenario.

 1. one throttled instance (2 concurrent activities) + a burst of N files -> the activity backlog grows;
 2. after --grow-seconds, 3 extra workers join (32 concurrent activities each) -> the backlog drains;
 3. prints the timeline (backlog, backlog age, throughput) and checks that everything was consumed.

Usage: scripts/backlog_test.py [--files 3000] [--grow-seconds 60]
"""
import argparse, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import chaos_test as c

c.INSTANCES = {
    "slow-api": {"port": 5055, "env": {"Worker__MaxConcurrentActivities": "2", "Worker__MaxConcurrentWorkflowTasks": "2"}},
    "w1": {"port": 5056, "env": {"Watcher__AutoStart": "false"}},
    "w2": {"port": 5057, "env": {"Watcher__AutoStart": "false"}},
    "w3": {"port": 5058, "env": {"Watcher__AutoStart": "false"}},
}


def sample(t0, last):
    s = c.stats()
    if not s:
        return last
    o, q = s["objects"], s["taskQueues"]
    act, wf = q["file-ingestion/Activity"], q["file-ingestion/Workflow"]
    done = o["processed/"] + o["invalid/"]
    rate = (done - last[1]) / max(1e-6, time.time() - last[0]) * 3600
    c.log(f"t={time.time() - t0:5.0f}s done={done:5} incoming={o['incoming/']:5} processing={o['processing/']:5} "
          f"| activity backlog={act.get('backlog', '?'):5} age={act.get('backlogAgeSeconds', 0):6.1f}s "
          f"| workflow backlog={wf.get('backlog', '?'):5} | {rate:7.0f} files/h")
    return (time.time(), done, s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", type=int, default=3000)
    ap.add_argument("--rows", type=int, default=50)
    ap.add_argument("--grow-seconds", type=int, default=60)
    args = ap.parse_args()
    os.makedirs(c.LOGS, exist_ok=True)

    c.start("slow-api", 0)
    if not c.wait_healthy(5055):
        sys.exit("instance did not start")
    c.PREFIX = f"backlog-{int(time.time())}"
    gen = c.http("POST", "/api/simulation/files", {"count": args.files, "rowsPerFile": args.rows, "invalidRatio": 0.02,
                                                   "poisonRatio": 0, "prefix": c.PREFIX}, timeout=600)
    c.log(f"burst of {gen['generated']} files, 1 worker limited to 2 concurrent activities")
    t0 = time.time()
    last = (t0, 0, None)
    scaled = False
    while True:
        time.sleep(5)
        last = sample(t0, last)
        s = last[2]
        if not scaled and time.time() - t0 >= args.grow_seconds:
            c.log(">>> scaling out: +3 workers (32 concurrent activities each)")
            for name in ("w1", "w2", "w3"):
                c.start(name, 0)
            scaled = True
        if s and s["objects"]["incoming/"] == 0 and s["objects"]["processing/"] == 0 and s["workflows"]["FileIngestion.running"] == 0:
            break
    elapsed = time.time() - t0
    o = s["objects"]
    ok = o["processed/"] + o["invalid/"] == args.files
    c.log(f"drained {args.files} files in {elapsed:.0f}s ({args.files / elapsed * 3600:.0f} files/h), all consumed: {ok}")
    for name in list(c.INSTANCES):
        c.kill(name)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
