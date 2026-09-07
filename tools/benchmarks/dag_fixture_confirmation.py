# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Fork-only, fixed-protocol confirmation for the finished DAG fixture correction."""

import argparse
import hashlib
import json
import math
import os
import pathlib
import platform
import shutil
import signal
import statistics
import subprocess
import tempfile
import threading
import time
import zipfile


FLAGS = ['-Xms4g', '-Xmx4g', '-XX:+UseG1GC', '-XX:+AlwaysPreTouch',
         '-XX:+DisableExplicitGC', '-XX:ActiveProcessorCount=4',
         '-Djava.net.preferIPv4Stack=true']
PREFIX = 'org/apache/seatunnel/benchmark/'
ALLOWED_CLASSES = {PREFIX + name for name in [
    'IMapDagStorageBenchmark.class',
    'storage/imap/IMapDagStorageBenchmarkWorkload.class',
    'jmh_generated/IMapDagStorageBenchmark_finishedJobDagStore_jmhTest.class',
    'jmh_generated/IMapDagStorageBenchmark_finishedJobDagLoad_jmhTest.class']}
CONTRACT = '''
public class DagContract {
    public static void main(String[] args) throws Exception {
        Class<?> state = Class.forName("org.apache.seatunnel.benchmark.storage.imap.IMapDagStorageBenchmarkWorkload");
        Class<?> bench = Class.forName("org.apache.seatunnel.benchmark.IMapDagStorageBenchmark");
        java.lang.reflect.Method method = bench.getDeclaredMethod("finishedJobDagStore", state);
        org.openjdk.jmh.annotations.Mode[] modes = method.getAnnotation(org.openjdk.jmh.annotations.BenchmarkMode.class).value();
        if (state.getField("STORE_OPERATIONS_PER_INVOCATION").getInt(null) != 100
                || method.getAnnotation(org.openjdk.jmh.annotations.OperationsPerInvocation.class).value() != 100
                || modes.length != 1 || modes[0] != org.openjdk.jmh.annotations.Mode.SingleShotTime) {
            throw new IllegalStateException("Unexpected benchmark contract");
        }
    }
}
'''


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def sha256(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1048576), b''):
            result.update(block)
    return result.hexdigest()


def check_classes(baseline, candidate):
    with zipfile.ZipFile(baseline) as old, zipfile.ZipFile(candidate) as new:
        old_names, new_names = set(old.namelist()), set(new.namelist())
        require(old_names == new_names, 'Packaged entry inventory changed')
        changed = sorted(name for name in old_names if not name.endswith('/')
                         and old.read(name) != new.read(name))
    require(changed and set(changed) <= ALLOWED_CLASSES,
            'Unexpected packaged differences: ' + repr(changed))
    return changed


def plan(version):
    cells = [(1, 100, ['baseline', 'candidate', 'candidate', 'baseline'])]
    if version == '11':
        cells.append((100, 0, ['candidate', 'baseline', 'baseline', 'candidate']))
    for pipelines, retained, roles in cells:
        for index, role in enumerate(roles):
            yield pipelines, retained, role, f'java{version}-p{pipelines}-s{retained}-{index}-{role}'


def snapshot():
    memory = {line.split(':')[0]: int(line.split()[1])
              for line in pathlib.Path('/proc/meminfo').read_text().splitlines()}
    vm = {line.split()[0]: int(line.split()[1])
          for line in pathlib.Path('/proc/vmstat').read_text().splitlines()}
    return {'epoch': time.time(), 'memory_available_kib': memory['MemAvailable'],
            'swap_used_kib': memory['SwapTotal'] - memory['SwapFree'],
            'pswpin': vm['pswpin'], 'pswpout': vm['pswpout'], 'load': os.getloadavg(),
            'cpu_ticks': [int(x) for x in pathlib.Path('/proc/stat').read_text().splitlines()[0].split()[1:9]],
            'memory_pressure': pathlib.Path('/proc/pressure/memory').read_text()}


def quiet_host(output):
    # Host gate occurs before any JMH JVM, never between its warmup/measurement iterations.
    samples = []
    for _ in range(24):
        before = snapshot()
        time.sleep(5)
        after = snapshot()
        samples += [before, after]
        save(output / 'host-gate.json', samples)
        elapsed = sum(after['cpu_ticks']) - sum(before['cpu_ticks'])
        busy = 1 - (after['cpu_ticks'][3] - before['cpu_ticks'][3]) / max(elapsed, 1)
        if (busy <= 0.10 and after['memory_available_kib'] >= 6 * 1024 * 1024
                and (before['pswpin'], before['pswpout']) == (after['pswpin'], after['pswpout'])):
            return
    raise RuntimeError('Host did not become build-idle with 6 GiB available and no active paging')


def validate(record, pipelines, retained):
    expected = {'benchmark': 'org.apache.seatunnel.benchmark.IMapDagStorageBenchmark.finishedJobDagStore',
                'jmhVersion': '1.37', 'mode': 'ss', 'forks': 3, 'threads': 1,
                'warmupIterations': 3, 'measurementIterations': 5,
                'warmupBatchSize': 1, 'measurementBatchSize': 1, 'jvmArgs': FLAGS,
                'params': {'pipelineCount': str(pipelines), 'storedDagCount': str(retained)}}
    for name, value in expected.items():
        require(record.get(name) == value, 'Incorrect ' + name)
    metric = record['primaryMetric']
    require(metric['scoreUnit'] == 'us/op', 'Unexpected units')
    raw = metric['rawData']
    require(len(raw) == 3 and all(len(row) == 5 for row in raw), 'Incomplete samples')
    require(all(math.isfinite(x) and x > 0 for row in raw for x in row), 'Invalid sample')
    require(math.isclose(metric['score'], statistics.mean(x for row in raw for x in row), rel_tol=1e-9),
            'Score and raw samples disagree')
    return raw


def collect(args):
    require(platform.system() == 'Linux' and platform.machine() == 'x86_64', 'Native Linux x86_64 required')
    for variable in ['JAVA_TOOL_OPTIONS', '_JAVA_OPTIONS', 'JDK_JAVA_OPTIONS']:
        require(not os.environ.get(variable), 'Unexpected injected JVM options: ' + variable)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    artifacts = {'baseline': args.baseline.resolve(), 'candidate': args.candidate.resolve()}
    java = str(pathlib.Path(shutil.which('java')).resolve())
    javac = str(pathlib.Path(java).with_name('javac'))
    version = subprocess.run([java, '-version'], check=True, capture_output=True, text=True).stderr
    require(('version "1.8.' if args.java_version == '8' else 'version "11.') in version, 'Incorrect native JDK')
    hashes = {role: sha256(path) for role, path in artifacts.items()}
    identity = {'jar_sha256': hashes, 'java_version': version, 'machine': platform.uname()._asdict(),
                'cpu_count': os.cpu_count(), 'changed_jar_entries': check_classes(*artifacts.values()),
                'protocol': {'forks': 3, 'warmups': 3, 'measurements': 5, 'writes_per_batch': 100},
                'revisions': {key: os.environ.get(key) for key in ['BASELINE_SHA', 'CANDIDATE_SHA', 'GITHUB_SHA', 'GITHUB_RUN_ID']}}
    save(output / 'artifacts.json', identity)
    for jar in artifacts.values():
        with tempfile.TemporaryDirectory() as directory:
            source = pathlib.Path(directory) / 'DagContract.java'
            source.write_text(CONTRACT)
            subprocess.run([javac, '-cp', str(jar), str(source)], check=True)
            subprocess.run([java, '-cp', directory + os.pathsep + str(jar), 'DagContract'], check=True)
    quiet_host(output)
    summary = {'complete': False, 'runs': [], 'expected_runs': len(list(plan(args.java_version)))}
    save(output / 'summary.json', summary)
    for pipelines, retained, role, label in plan(args.java_version):
        target = output / label
        target.mkdir()
        command = [java, '-jar', str(artifacts[role]), 'IMapDagStorageBenchmark.finishedJobDagStore$',
                   '-jvm', java, '-f', '3', '-wi', '3', '-i', '5', '-wf', '0', '-bs', '1', '-wbs', '1',
                   '-w', '1s', '-r', '1s', '-t', '1', '-foe', 'true',
                   '-p', 'pipelineCount=' + str(pipelines), '-p', 'storedDagCount=' + str(retained),
                   '-rf', 'json', '-rff', str(target / 'results.json')]
        before = snapshot()
        meta = {'label': label, 'role': role, 'command': command, 'before': before, 'jar_sha256': hashes[role]}
        save(target / 'manifest.json', meta)
        print('START ' + label, flush=True)
        stop = threading.Event()
        monitor_errors = []

        def monitor():
            try:
                with (target / 'host.jsonl').open('w') as stream:
                    while not stop.wait(1):
                        stream.write(json.dumps(snapshot()) + '\n')
                        stream.flush()
            except BaseException as error:
                monitor_errors.append(repr(error))

        watcher = threading.Thread(target=monitor)
        watcher.start()
        try:
            with (target / 'run.log').open('w') as log:
                process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                           cwd=target, start_new_session=True)
                try:
                    code = process.wait(timeout=1200)
                except BaseException:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
                    raise
            after = snapshot()
            stop.set()
            watcher.join()
            meta.update({'exit_code': code, 'after': after})
            require(code == 0, 'JMH failed')
            require(not monitor_errors, 'Host monitoring failed: ' + repr(monitor_errors))
            require((before['pswpin'], before['pswpout']) == (after['pswpin'], after['pswpout']),
                    'Active paging invalidates isolated timing confirmation')
            records = json.loads((target / 'results.json').read_text())
            require(len(records) == 1, 'Unexpected benchmark count')
            raw = validate(records[0], pipelines, retained)
            meta.update({'valid': True, 'result_sha256': sha256(target / 'results.json'),
                         'log_sha256': sha256(target / 'run.log'), 'raw_us_per_dag': raw,
                         'fork_means': [statistics.mean(row) for row in raw],
                         'fork_sd': [statistics.stdev(row) for row in raw]})
            summary['runs'].append(meta)
            save(output / 'summary.json', summary)
            print('PASS ' + label + ' ' + repr(raw), flush=True)
        except BaseException as error:
            meta.update({'valid': False, 'failure': repr(error)})
            raise
        finally:
            stop.set()
            watcher.join()
            meta['monitor_errors'] = monitor_errors
            save(target / 'manifest.json', meta)
    require(all(sha256(artifacts[role]) == expected for role, expected in hashes.items()), 'Artifact drift')
    summary['complete'] = True
    save(output / 'summary.json', summary)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--baseline', type=pathlib.Path, required=True)
    parser.add_argument('--candidate', type=pathlib.Path, required=True)
    parser.add_argument('--java-version', choices=['8', '11'], required=True)
    parser.add_argument('--output', type=pathlib.Path, required=True)
    collect(parser.parse_args())
