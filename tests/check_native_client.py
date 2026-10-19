#!/usr/bin/env python3
"""Run the native C client against independent Python transport fixtures."""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import os
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
from collections.abc import Iterator
from pathlib import Path


@contextlib.contextmanager
def worker(*args: str, tls: bool = False) -> Iterator[tuple[str, str | None]]:
    proc = subprocess.Popen(
        [sys.executable, "-m", "vgi_rpc.conformance.client_worker", *args], stdout=subprocess.PIPE,
        env={**os.environ, "VGI_RPC_SHM_MIN_BATCH_BYTES": "0"},
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith(("PORT:", "UNIX:", "TCP:")), line
        target = line.split(":", 1)[1]
        ca = None
        if tls:
            line = proc.stdout.readline().decode().strip()
            assert line.startswith("TLS-CA:"), line
            ca = line.split(":", 1)[1]
        yield target, ca
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-dir", type=Path, default=Path("build"))
    args = parser.parse_args()
    executable = str((args.build_dir / "vgi-rpc-client-integration").resolve())

    def run(*argv: str, shm: bool = False) -> None:
        subprocess.run(
            [executable, *argv], check=True, timeout=90,
            env={**os.environ, "NO_PROXY": "localhost,127.0.0.1,127.0.0.2",
                 **({"VGI_RPC_C_TEST_SHM": "1", "VGI_RPC_SHM_MIN_BATCH_BYTES": "0"} if shm else {})},
        )

    run("stdio", sys.executable)
    run("stdio", sys.executable, shm=True)
    with tempfile.TemporaryDirectory(prefix="vgi-c-client-", dir="/tmp") as directory:
        with worker("--unix", str(Path(directory) / "worker.sock")) as (path, _):
            run("unix", path)
            run("unix", path, shm=True)
    with worker("--tcp", "127.0.0.1:0") as (address, _):
        run("tcp", address)
    for extra in ((), ("--producer-turn-bytes", "65536"), ("--external",)):
        with worker("--http", "0", "--prefix", "/vgi", *extra) as (port, _):
            run("http", f"http://127.0.0.1:{port}/vgi")
    with worker("--http", "0", "--prefix", "/vgi", "--tls", tls=True) as (port, ca):
        assert ca
        run("untrusted", f"https://127.0.0.1:{port}/vgi")
        run("http", f"https://127.0.0.1:{port}/vgi", ca)
    # A trusted certificate for another hostname must fail before any RPC.
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    with tempfile.TemporaryDirectory(prefix="vgi-c-tls-") as directory:
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "wrong.invalid")])
        now = dt.datetime.now(dt.UTC)
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - dt.timedelta(minutes=5))
            .not_valid_after(now + dt.timedelta(days=1))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName("wrong.invalid")]), critical=False)
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
            .sign(key, hashes.SHA256())
        )
        cert_path, key_path = Path(directory) / "ca.pem", Path(directory) / "key.pem"
        cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        key_path.write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
            )
        )
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(cert_path, key_path)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            listener.settimeout(10)

            def handshake():
                with listener.accept()[0] as peer:
                    with contextlib.suppress(ssl.SSLError):
                        with context.wrap_socket(peer, server_side=True):
                            pass

            thread = threading.Thread(target=handshake, daemon=True)
            thread.start()
            run("untrusted", f"https://127.0.0.1:{listener.getsockname()[1]}/vgi", str(cert_path))
            thread.join(timeout=10)
            assert not thread.is_alive()
    print("Native client: stdio, Unix, TCP, HTTP, buffered init, external storage, and TLS passed")


if __name__ == "__main__":
    main()
