#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Sequential, fork-preserving storage lifecycle collection. No automatic adoption decision."""
import argparse
import hashlib
import json
import math
import os
import pathlib
import platform
import random
import signal
import statistics
import subprocess
import time
import zipfile

PREFIX = "org.apache.seatunnel.benchmark.IMapDagStorageLifecycleBenchmark."
METHODS = ("finishedJobDagStartupEndToEnd", "finishedJobDagStoreAfterReadiness")
FLAGS = ["-Xms4g", "-Xmx4g", "-XX:+UseG1GC", "-XX:+AlwaysPreTouch",
         "-XX:+DisableExplicitGC", "-XX:ActiveProcessorCount=4",
         "-Djava.net.preferIPv4Stack=true"]


def execute(command, stream, timeout):
    """Terminate the JMH fork process group too, not only the launcher, on failure."""
    process = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT,
                               start_new_session=True)
    try:
        code = process.wait(timeout=timeout)
        if code:
            raise subprocess.CalledProcessError(code, command)
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
        finally:
            # The launcher may exit while a fork still ignores SIGTERM.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
        raise


def sha256(path):
    digest = hashlib.sha256()
    with pathlib.Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def production_digest(path):
    """Only benchmark classes and JMH indexes may differ in this fixture study."""
    digest = hashlib.sha256()
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)):
            raise ValueError("Duplicate ZIP entries")
        for name in sorted(names):
            if name.endswith("/") or name.startswith("org/apache/seatunnel/benchmark/"):
                continue
            if name in ("META-INF/BenchmarkList", "META-INF/CompilerHints"):
                continue
            digest.update(name.encode("utf-8") + b"\0")
            digest.update(hashlib.sha256(archive.read(name)).digest())
    return digest.hexdigest()


def plan(purpose, blocks, seed):
    if blocks < 1:
        raise ValueError("blocks must be positive")
    cells = [(method, p, s) for method in METHODS for p in (1, 10, 100) for s in (0, 100)]
    random.Random(seed).shuffle(cells)
    rows = []
    for block in range(1 if purpose == "smoke" else blocks):
        for method, p, s in (cells if block % 2 == 0 else list(reversed(cells))):
            roles = ["candidate"] if purpose == "smoke" else (
                ["baseline", "candidate", "candidate", "baseline"] if block % 2 == 0
                else ["candidate", "baseline", "baseline", "candidate"])
            for position, role in enumerate(roles):
                startup = method == METHODS[0]
                rows.append(dict(block=block, position=position, role=role, method=method,
                                 pipelineCount=p, storedDagCount=s,
                                 warmup=0 if startup else (1 if purpose == "smoke" else 3),
                                 iterations=1 if startup or purpose == "smoke" else 5))
    return rows


def validate_result(data, row):
    if len(data) != 1:
        raise ValueError("Exactly one benchmark result required")
    result = data[0]
    expected = dict(benchmark=PREFIX + row["method"], mode="ss", threads=1, forks=1,
                    warmupIterations=row["warmup"], measurementIterations=row["iterations"],
                    warmupBatchSize=1, measurementBatchSize=1,
                    params={"pipelineCount": str(row["pipelineCount"]),
                            "storedDagCount": str(row["storedDagCount"])})
    for key, value in expected.items():
        if result.get(key) != value:
            raise ValueError("Unexpected {}: {}".format(key, result.get(key)))
    if result.get("jvmArgs") != FLAGS:
        raise ValueError("Unexpected JVM flags")
    metric = result["primaryMetric"]
    unit = "ms/op" if row["method"] == METHODS[0] else "us/op"
    if metric["scoreUnit"] != unit:
        raise ValueError("Unexpected score unit")
    raw = metric["rawData"]
    if len(raw) != 1 or len(raw[0]) != row["iterations"]:
        raise ValueError("Missing fork or iteration")
    samples = raw[0]
    if any(not isinstance(x, (int, float)) or not math.isfinite(x) or x <= 0 for x in samples):
        raise ValueError("Invalid measured sample")
    mean = statistics.mean(samples)
    if not math.isfinite(metric["score"]) or not math.isclose(mean, metric["score"], rel_tol=1e-6):
        raise ValueError("Score does not match raw samples")
    sd = statistics.stdev(samples) if len(samples) > 1 else None
    return dict(mean=mean, median=statistics.median(samples), sd=sd,
                cv=None if sd is None else sd / mean, samples=samples,
                unit=unit, jmh_error=metric.get("scoreError"),
                jdk=result["jdkVersion"], vm=result["vmVersion"])


def run(args):
    if args.purpose == "comparison" and platform.system() != "Linux":
        raise ValueError("Comparison requires Linux; use smoke for functional checks elsewhere")
    if any(os.environ.get(key) for key in ("JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS")):
        raise ValueError("Implicit JVM option environment variables must be unset")
    output = pathlib.Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    try:
        collect(args, output)
    except BaseException as error:
        (output / "collection-failure.json").write_text(json.dumps(
            {"status": "failed", "error": str(error)}, indent=2) + "\n")
        raise


def collect(args, output):
    jars = {role: pathlib.Path(getattr(args, role)).resolve() for role in ("baseline", "candidate")}
    hashes = {role: sha256(path) for role, path in jars.items()}
    if args.purpose == "comparison" and hashes["baseline"] == hashes["candidate"] and not args.allow_identical:
        raise ValueError("Identical artifacts require explicit --allow-identical for A/A calibration")
    if production_digest(jars["baseline"]) != production_digest(jars["candidate"]):
        raise ValueError("Non-benchmark artifact entries differ; not a fixture-only comparison")
    rows = plan(args.purpose, args.blocks, args.seed)
    metadata = dict(purpose=args.purpose, comparison="A/A" if len(set(hashes.values())) == 1 else "A/B",
                    platform=platform.platform(), architecture=platform.machine(),
                    container=pathlib.Path("/.dockerenv").exists(),
                    seed=args.seed, blocks=args.blocks,
                    collector_sha256=sha256(__file__),
                    cpu_count=os.cpu_count(), flags=FLAGS, artifacts=hashes,
                    runner={key: os.environ.get(key) for key in
                            ("RUNNER_OS", "RUNNER_ARCH", "ImageOS", "ImageVersion", "GITHUB_RUN_ID",
                             "BASELINE_SHA", "CANDIDATE_SHA")},
                    java=subprocess.check_output([args.java, "-version"], stderr=subprocess.STDOUT,
                                                 text=True), plan=rows, status="running", results=[])
    cpuinfo = pathlib.Path("/proc/cpuinfo")
    if cpuinfo.exists():
        (output / "cpuinfo.txt").write_text(cpuinfo.read_text())
    manifest = output / "manifest.json"
    def save():
        manifest.write_text(json.dumps(metadata, indent=2, allow_nan=False) + "\n")
    save()
    try:
        runtime = None
        for index, row in enumerate(rows):
            raw = output / ("{:03d}.json".format(index))
            log = output / ("{:03d}.log".format(index))
            command = [args.java, "-jar", str(jars[row["role"]]), PREFIX + row["method"] + "$",
                       "-f", "1", "-wf", "0", "-t", "1", "-bm", "ss", "-bs", "1", "-wbs", "1",
                       "-wi", str(row["warmup"]), "-i", str(row["iterations"]),
                       "-p", "pipelineCount={}".format(row["pipelineCount"]),
                       "-p", "storedDagCount={}".format(row["storedDagCount"]),
                       "-foe", "true", "-rf", "json", "-rff", str(raw)]
            entry = dict(row, command=command, started=time.time(), load_before=os.getloadavg())
            metadata["results"].append(entry)
            save()
            with log.open("w") as stream:
                execute(command, stream, args.timeout)
            summary = validate_result(json.loads(raw.read_text()), row)
            fingerprint = (summary["jdk"], summary["vm"])
            if runtime is not None and runtime != fingerprint:
                raise ValueError("JVM identity changed during collection")
            runtime = fingerprint
            entry.update(summary=summary, raw_sha256=sha256(raw), log_sha256=sha256(log),
                         elapsed=time.time() - entry["started"], load_after=os.getloadavg())
            save()
        if hashes != {role: sha256(path) for role, path in jars.items()}:
            raise ValueError("Artifacts changed during collection")
        metadata["status"] = "complete"
    except BaseException as error:
        metadata["status"] = "failed"
        metadata["error"] = str(error)
        raise
    finally:
        save()
    summarize(output)


def summarize(output):
    """Descriptive block contrasts; iterations are never treated as independent JVM forks."""
    output = pathlib.Path(output)
    manifest = json.loads((output / "manifest.json").read_text())
    if manifest["status"] != "complete" or len(manifest["results"]) != len(manifest["plan"]):
        raise ValueError("Incomplete collection")
    groups = {}
    for index, (row, entry) in enumerate(zip(manifest["plan"], manifest["results"])):
        if any(entry.get(key) != value for key, value in row.items()):
            raise ValueError("Collection plan mismatch")
        raw = output / ("{:03d}.json".format(index))
        if sha256(raw) != entry["raw_sha256"]:
            raise ValueError("Result checksum mismatch")
        if sha256(output / ("{:03d}.log".format(index))) != entry["log_sha256"]:
            raise ValueError("Log checksum mismatch")
        stats = validate_result(json.loads(raw.read_text()), row)
        key = (row["method"], row["pipelineCount"], row["storedDagCount"], row["block"])
        groups.setdefault(key, {}).setdefault(row["role"], []).append(stats)
    contrasts = []
    for key, roles in groups.items():
        item = dict(method=key[0], pipelineCount=key[1], storedDagCount=key[2], block=key[3],
                    roles={})
        for role, forks in roles.items():
            means = [fork["mean"] for fork in forks]
            sd = statistics.stdev(means) if len(means) > 1 else None
            item["roles"][role] = dict(forks=len(means), mean=statistics.mean(means),
                                      median=statistics.median(means), fork_means=means,
                                      between_fork_sd=sd,
                                      between_fork_cv=None if sd is None else sd / statistics.mean(means),
                                      within_fork_sd=[fork["sd"] for fork in forks],
                                      within_fork_cv=[fork["cv"] for fork in forks])
        if "baseline" in roles:
            item["candidate_baseline_mean_ratio"] = (
                item["roles"]["candidate"]["mean"] / item["roles"]["baseline"]["mean"])
        contrasts.append(item)
    (output / "analysis.json").write_text(json.dumps(dict(
        purpose=manifest["purpose"], comparison=manifest["comparison"],
        inference="Descriptive only. No production speedup, tail-latency or non-regression claim.",
        blocks=contrasts), indent=2, allow_nan=False) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--java", default="java")
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--purpose", choices=("smoke", "comparison"), required=True)
    parser.add_argument("--blocks", type=int, default=2)
    parser.add_argument("--seed", type=int, default=12060)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--allow-identical", action="store_true")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
