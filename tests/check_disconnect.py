"""A client that disconnects mid-response must not leave a spinning server thread.

Starts the conformance worker on a Unix socket with a short idle timeout, has a
client process start a large producer stream and get killed while the server
is still writing, then asserts that the server's CPU use returns to idle and
that the idle timeout fires (the process exits), so no connection thread is
left behind. Before the fix, nanoarrow's FILE output stream reported every
write after the failed one as a successful zero-byte write and the write loop
spun at 100% CPU forever.
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLIENT = """
import sys
from vgi_rpc.conformance._protocol import ConformanceService
from vgi_rpc.rpc import unix_connect
with unix_connect(ConformanceService, sys.argv[1]) as service:
    stream = iter(service.produce_large_batches(rows_per_batch=20_000_000, batch_count=64))
    print("STARTED", flush=True)
    next(stream)  # the parent stops this process while the server writes ~160 MB
"""


def cpu_seconds(pid: int) -> float:
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, default=ROOT / "build")
    args = parser.parse_args()
    worker_bin = args.build_dir / "conformance" / "conformance_worker"
    with tempfile.TemporaryDirectory() as directory:
        path = str(Path(directory) / "worker.sock")
        worker = subprocess.Popen([str(worker_bin), "--unix", path, "--idle-timeout", "2"])
        try:
            for _ in range(100):
                if os.path.exists(path):
                    break
                time.sleep(0.05)
            client = subprocess.Popen([sys.executable, "-c", CLIENT, path], stdout=subprocess.PIPE, text=True)
            assert client.stdout is not None
            assert client.stdout.readline().strip() == "STARTED", "client never started the stream"
            time.sleep(0.05)
            client.send_signal(signal.SIGSTOP)  # mid-read: the server blocks in write()
            time.sleep(1.0)
            client.send_signal(signal.SIGKILL)
            client.wait(timeout=10)
            time.sleep(0.5)
            if worker.poll() is None:
                before = cpu_seconds(worker.pid)
                time.sleep(1.0)
                if worker.poll() is None:
                    used = cpu_seconds(worker.pid) - before
                    assert used < 0.3, f"server used {used:.2f} s of CPU in 1 s after the disconnect (spinning)"
            try:
                code = worker.wait(timeout=15)
            except subprocess.TimeoutExpired:
                raise AssertionError("the idle timeout never fired: a connection thread is still alive") from None
            assert code == 0, f"worker exited {code}"
        finally:
            if worker.poll() is None:
                worker.kill()
                worker.wait()
    print("Disconnect mid-response: the connection thread exits, CPU returns to idle, idle timeout fires")
    return 0


if __name__ == "__main__":
    sys.exit(main())
