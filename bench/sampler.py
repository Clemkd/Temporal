#!/usr/bin/env python3
"""
Samples every second until killed:
  - CPU (% of one core) and RAM (MB, without page cache) of the Temporal and Postgres containers (Docker API);
  - Postgres activity from pg_stat_statements / pg_stat_database (interval deltas):
      INSERT calls/s and mean INSERT time (ms), UPDATE mean time, commits/s, rows inserted/s, DB size.
Usage: sampler.py <out.csv> [--pg-port 55433]
"""
import http.client, json, socket, sys, time
import psycopg2

OUT = sys.argv[1]
PG_PORT = int(sys.argv[sys.argv.index("--pg-port") + 1]) if "--pg-port" in sys.argv else 55433
CONTAINERS = {"temporal": "temporal-bench-temporal-1", "postgres": "temporal-bench-postgres-1"}


class UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self):
        super().__init__("localhost")

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect("/var/run/docker.sock")


def container_stats(name):
    c = UnixHTTPConnection()
    c.request("GET", f"/containers/{name}/stats?stream=false&one-shot=true")
    s = json.loads(c.getresponse().read())
    c.close()
    mem = s["memory_stats"]
    cache = mem.get("stats", {}).get("total_inactive_file", mem.get("stats", {}).get("inactive_file", 0))
    return s["cpu_stats"]["cpu_usage"]["total_usage"], (mem.get("usage", 0) - cache) / 1024 / 1024


PG_SQL = """
SELECT
  coalesce(sum(calls) FILTER (WHERE query ILIKE 'insert%'), 0),
  coalesce(sum(total_exec_time) FILTER (WHERE query ILIKE 'insert%'), 0),
  coalesce(sum(calls) FILTER (WHERE query ILIKE 'update%'), 0),
  coalesce(sum(total_exec_time) FILTER (WHERE query ILIKE 'update%'), 0),
  (SELECT sum(xact_commit) FROM pg_stat_database),
  (SELECT sum(tup_inserted) FROM pg_stat_database),
  (SELECT sum(pg_database_size(datname)) FROM pg_database WHERE datname LIKE 'temporal%')
FROM pg_stat_statements
"""


def main():
    pg = psycopg2.connect(host="localhost", port=PG_PORT, user="temporal", password="temporal", dbname="postgres")
    pg.autocommit = True
    cur = pg.cursor()
    prev = None
    t0 = time.time()
    with open(OUT, "w") as f:
        f.write("ts,elapsed_s,temporal_cpu_pct,temporal_mem_mb,postgres_cpu_pct,postgres_mem_mb,"
                "insert_per_s,insert_mean_ms,update_per_s,update_mean_ms,commits_per_s,rows_inserted_per_s,db_size_mb\n")
        next_tick = time.time()
        while True:
            now = time.time()
            tc, tm = container_stats(CONTAINERS["temporal"])
            pc, pm = container_stats(CONTAINERS["postgres"])
            cur.execute(PG_SQL)
            ins_calls, ins_time, upd_calls, upd_time, commits, tup_ins, size = [float(x or 0) for x in cur.fetchone()]
            sample = (now, tc, pc, ins_calls, ins_time, upd_calls, upd_time, commits, tup_ins)
            if prev:
                dt = now - prev[0]
                d = [a - b for a, b in zip(sample, prev)]
                row = [
                    f"{now:.1f}", f"{now - t0:.1f}",
                    f"{d[1] / 1e9 / dt * 100:.1f}", f"{tm:.0f}",
                    f"{d[2] / 1e9 / dt * 100:.1f}", f"{pm:.0f}",
                    f"{d[3] / dt:.0f}", f"{(d[4] / d[3]) if d[3] > 0 else 0:.4f}",
                    f"{d[5] / dt:.0f}", f"{(d[6] / d[5]) if d[5] > 0 else 0:.4f}",
                    f"{d[7] / dt:.0f}", f"{d[8] / dt:.0f}", f"{size / 1024 / 1024:.0f}",
                ]
                f.write(",".join(row) + "\n")
                f.flush()
            prev = sample
            next_tick += 1
            time.sleep(max(0, next_tick - time.time()))


if __name__ == "__main__":
    main()
