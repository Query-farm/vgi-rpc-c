#!/usr/bin/env python3
"""Validate actual OTLP/HTTP output against protobuf schemas and RPC causality."""

from __future__ import annotations

import argparse
import base64
import copy
import json
import os
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from google.protobuf.json_format import ParseDict
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest


def protobuf_json(value):
    """OTLP/JSON uses hex IDs; protobuf's generic JSON decoder expects base64."""
    if isinstance(value, list):
        return [protobuf_json(item) for item in value]
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            if key in {"traceId", "spanId", "parentSpanId"}:
                assert len(item) == (32 if key == "traceId" else 16)
                result[key] = base64.b64encode(bytes.fromhex(item)).decode()
            else:
                result[key] = protobuf_json(item)
        return result
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-dir", type=Path, default=Path("build"))
    args = parser.parse_args()
    test = str((args.build_dir / "vgi-rpc-features-test").resolve())
    worker = str((args.build_dir / "conformance/conformance_worker").resolve())
    records = []
    errors = []
    lock = threading.Lock()
    response_mode = "success"
    requests = {}

    class Collector(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            body = b"{}"
            try:
                assert self.headers["Content-Type"] == "application/json"
                data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                assert self.path in {"/v1/traces", "/v1/metrics"}
                message = ExportTraceServiceRequest() if self.path == "/v1/traces" else ExportMetricsServiceRequest()
                ParseDict(protobuf_json(copy.deepcopy(data)), message)
                assert message.ByteSize() > 0
                with lock:
                    records.append((self.path, data))
                    requests[self.path] = requests.get(self.path, 0) + 1
                    attempt = requests[self.path]
                status = 200
                if response_mode == "transient" and attempt == 1:
                    status = 503
                elif response_mode == "permanent":
                    status = 400
                elif response_mode == "partial":
                    key = "rejectedSpans" if self.path == "/v1/traces" else "rejectedDataPoints"
                    body = json.dumps({"partialSuccess": {key: "1", "errorMessage": "rejected"}}).encode()
                self.send_response(status)
            except Exception as error:
                with lock:
                    errors.append(error)
                self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    with ThreadingHTTPServer(("127.0.0.1", 0), Collector) as collector:
        thread = threading.Thread(target=collector.serve_forever, daemon=True)
        thread.start()
        endpoint = f"http://127.0.0.1:{collector.server_port}"
        env = {**os.environ, "VGI_RPC_OTEL_ENDPOINT": endpoint, "NO_PROXY": "127.0.0.1,localhost"}
        for transport in ("stdio", "http"):
            with lock:
                records.clear()
            server = None
            try:
                target = worker
                if transport == "http":
                    server = subprocess.Popen([worker, "--http"], env=env, stdout=subprocess.PIPE)
                    assert server.stdout is not None
                    line = server.stdout.readline().decode().strip()
                    assert line.startswith("PORT:"), line
                    target = f"http://127.0.0.1:{line.split(':')[1]}/vgi"
                subprocess.run([test, "--telemetry", endpoint, transport, target], env=env, check=True, timeout=30)
                # Client Flush is explicit; the independent HTTP server exports asynchronously.
                limit = time.monotonic() + 5
                while time.monotonic() < limit:
                    with lock:
                        snapshot = copy.deepcopy(records)
                    spans, counters, histograms = [], {}, {}
                    for path, data in snapshot:
                        if path == "/v1/traces":
                            for resource in data["resourceSpans"]:
                                for scope in resource["scopeSpans"]:
                                    spans.extend(scope["spans"])
                        else:
                            for resource in data["resourceMetrics"]:
                                for scope in resource["scopeMetrics"]:
                                    for metric in scope["metrics"]:
                                        name = metric["name"]
                                        kind = metric.get("sum") or metric.get("histogram")
                                        assert kind["aggregationTemporality"] == 1
                                        for point in kind["dataPoints"]:
                                            assert int(point["timeUnixNano"]) >= int(point["startTimeUnixNano"])
                                            if "sum" in metric:
                                                counters[name] = counters.get(name, 0) + int(point["asInt"])
                                            else:
                                                assert point["sum"] >= 0
                                                histograms[name] = histograms.get(name, 0) + int(point["count"])
                    if counters.get("rpc.server.requests", 0) >= 6:
                        break
                    time.sleep(0.02)
                assert not errors, errors
                clients = [span for span in spans if span["kind"] == 3]
                servers = [span for span in spans if span["kind"] == 2]
                assert len(clients) == 5, clients
                assert len(servers) >= 5, servers
                assert counters["rpc.client.requests"] == 6, counters
                assert counters["rpc.server.requests"] >= 6, counters
                for role in ("client", "server"):
                    assert counters[f"rpc.{role}.requests"] == histograms[f"rpc.{role}.duration"]
                parent_trace = "4bf92f3577b34da6a3ce929d0e0e4736"
                assert sum(span["traceId"] == parent_trace for span in clients) == 4
                assert all(span["traceId"] != "a" * 32 for span in spans)  # Unsampled calls still count in metrics.
                by_id = {span["spanId"]: span for span in clients}
                for span in servers:
                    parent = by_id[span["parentSpanId"]]
                    assert span["traceId"] == parent["traceId"]
                    assert int(span["endTimeUnixNano"]) >= int(span["startTimeUnixNano"])
                    if span["traceId"] == parent_trace:
                        assert span["traceState"] == "vendor=test"
                assert any(span["status"]["code"] == 2 for span in clients)
                assert any(span["status"]["code"] == 2 for span in servers)
                assert all("traced" not in json.dumps(span) and "logged" not in json.dumps(span) for span in spans)
                print(f"{transport}: OTLP protobuf validation, parentage, sampling, errors, metrics, logs passed")
            finally:
                if server:
                    server.terminate()
                    server.wait(timeout=10)
        delivery_env = {k: v for k, v in env.items() if k not in {"VGI_RPC_OTEL_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT"}}
        for response_mode in ("transient", "permanent", "partial"):
            with lock:
                requests.clear()
            failures = 0 if response_mode == "transient" else 2
            subprocess.run(
                [test, "--telemetry-delivery", endpoint, worker, str(failures)],
                env=delivery_env, check=True, timeout=15,
            )
            assert not errors, errors
            attempts = 2 if response_mode == "transient" else 1
            assert requests == {"/v1/traces": attempts, "/v1/metrics": attempts}, requests
            print(f"{response_mode}: collector retries/rejections leave RPC results intact")
        collector.shutdown()
        thread.join(timeout=5)


if __name__ == "__main__":
    main()
