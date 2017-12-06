import sys
import subprocess
import asyncio
import traceback
import argparse
from prometheus_client import Counter, Summary, Gauge, CollectorRegistry, push_to_gateway

parser = argparse.ArgumentParser(description='Execute a command and send results to a Prometheus push gateway')
parser.add_argument('host', help='Push gateway host including port')
parser.add_argument('job', help='Job name to use')
parser.add_argument('cmd', help='Command to run')
args = parser.parse_known_args(sys.argv[1:])

def quote(s: str) -> str:
    if ' ' in s:
        return '"' + s + '"'

    return s

async def runner_async() -> int:
    global args
    merged = [args[0].cmd] + args[1]
    cmd = ' '.join([quote(a) for a in merged])
    p = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await p.communicate()
    print(stdout.decode(), end='')
    print(stderr.decode(), end='', file=sys.stderr)
    return p.returncode

registry = CollectorRegistry()

errors = Counter('failures_total', 'Number of errors thrown by subprocess', registry=registry)
duration = Summary('duration_seconds', 'How long the subprocess took to run', registry=registry)

@duration.time()
@errors.count_exceptions()
def runner() -> int:
    loop = asyncio.get_event_loop()
    res = loop.run_until_complete(runner_async())
    loop.close()
    return res

res = 255
try:
    res = runner()
    if res == 0:
        g = Gauge('job_last_success_unixtime', 'Last time a batch job successfully finished', registry=registry)
        g.set_to_current_time()
    else:
        errors.inc()
except Exception:
    traceback.print_exc()

push_to_gateway(args[0].host, job=args[0].job, registry=registry)
exit(res)