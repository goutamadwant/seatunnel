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

import copy
import json
import signal
import subprocess
import tempfile
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch, call

import storage_lifecycle_matrix as matrix


class StorageLifecycleMatrixTest(unittest.TestCase):
    def fixture(self):
        row = matrix.plan("comparison", 1, 12060)[0]
        raw = [10.0] * row["iterations"]
        data = [dict(benchmark=matrix.PREFIX + row["method"], mode="ss", threads=1, forks=1,
                     warmupIterations=row["warmup"], measurementIterations=row["iterations"],
                     warmupBatchSize=1, measurementBatchSize=1, jvmArgs=matrix.FLAGS,
                     params={"pipelineCount": str(row["pipelineCount"]),
                             "storedDagCount": str(row["storedDagCount"])},
                     jdkVersion="11", vmVersion="11-test",
                     primaryMetric=dict(score=10.0, scoreError="NaN",
                                        scoreUnit="ms/op" if row["method"] == matrix.METHODS[0] else "us/op",
                                        rawData=[raw]))]
        return row, data

    def test_full_counterbalanced_plan(self):
        rows = matrix.plan("comparison", 2, 12060)
        self.assertEqual(96, len(rows))
        self.assertEqual(["baseline", "candidate", "candidate", "baseline"],
                         [x["role"] for x in rows[:4]])
        self.assertEqual(["candidate", "baseline", "baseline", "candidate"],
                         [x["role"] for x in rows[48:52]])
        self.assertEqual(rows[44]["method"], rows[48]["method"])
        self.assertEqual(12, len(matrix.plan("smoke", 2, 12060)))

    def test_startup_has_one_unwarmed_sample(self):
        for row in matrix.plan("comparison", 2, 0):
            if row["method"] == matrix.METHODS[0]:
                self.assertEqual((0, 1), (row["warmup"], row["iterations"]))

    def test_valid_result(self):
        row, data = self.fixture()
        self.assertEqual(10, matrix.validate_result(data, row)["mean"])

    def test_invalid_shape_identity_or_units_fail_closed(self):
        row, original = self.fixture()
        mutations = [
            lambda d: d.append(copy.deepcopy(d[0])),
            lambda d: d[0].update(forks=2),
            lambda d: d[0].update(mode="avgt"),
            lambda d: d[0].update(jvmArgs=[]),
            lambda d: d[0]["params"].update(pipelineCount="999"),
            lambda d: d[0]["primaryMetric"].update(scoreUnit="ops/s"),
            lambda d: d[0]["primaryMetric"].update(rawData=[[]]),
            lambda d: d[0]["primaryMetric"].update(rawData=[[float("nan")]]),
            lambda d: d[0]["primaryMetric"].update(score=20),
        ]
        for mutate in mutations:
            data = copy.deepcopy(original)
            mutate(data)
            with self.assertRaises(ValueError):
                matrix.validate_result(data, row)

    def test_single_sample_has_no_invented_dispersion(self):
        row = next(x for x in matrix.plan("comparison", 1, 1) if x["iterations"] == 1)
        _, data = self.fixture()
        data[0].update(benchmark=matrix.PREFIX + row["method"], warmupIterations=0,
                       measurementIterations=1,
                       params={"pipelineCount": str(row["pipelineCount"]),
                               "storedDagCount": str(row["storedDagCount"])})
        data[0]["primaryMetric"].update(rawData=[[10.0]], scoreUnit="ms/op")
        result = matrix.validate_result(data, row)
        self.assertIsNone(result["sd"])
        self.assertIsNone(result["cv"])

    def test_production_and_resource_identity_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = [Path(directory) / x for x in ("a.jar", "b.jar", "c.jar")]
            for index, path in enumerate(paths):
                with zipfile.ZipFile(path, "w") as archive:
                    archive.writestr("production.class", b"same")
                    archive.writestr("META-INF/services/service", b"changed" if index == 2 else b"same")
                    archive.writestr("org/apache/seatunnel/benchmark/Trial.class", str(index))
            self.assertEqual(matrix.production_digest(paths[0]), matrix.production_digest(paths[1]))
            self.assertNotEqual(matrix.production_digest(paths[0]), matrix.production_digest(paths[2]))

    def test_invalid_block_budget(self):
        with self.assertRaises(ValueError):
            matrix.plan("comparison", 0, 1)

    def test_failed_launcher_also_kills_remaining_fork_group(self):
        process = Mock(pid=123, **{"wait.return_value": 1})
        with patch.object(matrix.subprocess, "Popen", return_value=process), \
                patch.object(matrix.os, "killpg") as kill:
            with self.assertRaises(subprocess.CalledProcessError):
                matrix.execute(["java"], None, 1)
        self.assertEqual([call(123, signal.SIGTERM), call(123, signal.SIGKILL)], kill.call_args_list)

    def test_preflight_failure_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "collection"
            args = SimpleNamespace(purpose="smoke", output=str(output))
            with patch.dict(matrix.os.environ, {}, clear=True), \
                    patch.object(matrix, "collect", side_effect=ValueError("invalid artifact")):
                with self.assertRaisesRegex(ValueError, "invalid artifact"):
                    matrix.run(args)
            self.assertEqual("failed", json.loads((output / "collection-failure.json").read_text())["status"])

    def test_summary_rejects_incomplete_or_tampered_evidence(self):
        row, data = self.fixture()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            raw, log = output / "000.json", output / "000.log"
            raw.write_text(json.dumps(data))
            log.write_text("original log")
            manifest = dict(status="failed", purpose="smoke", comparison="A/A", plan=[row],
                            results=[dict(row, raw_sha256=matrix.sha256(raw), log_sha256=matrix.sha256(log))])
            path = output / "manifest.json"
            path.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "Incomplete"):
                matrix.summarize(output)
            manifest["status"] = "complete"
            # A smoke collection has only a candidate, not an A/B contrast.
            row["role"] = manifest["results"][0]["role"] = "candidate"
            path.write_text(json.dumps(manifest))
            matrix.summarize(output)
            self.assertTrue((output / "analysis.json").exists())
            log.write_text("changed log")
            with self.assertRaisesRegex(ValueError, "Log checksum"):
                matrix.summarize(output)
            log.write_text("original log")
            raw.write_text("[]")
            with self.assertRaisesRegex(ValueError, "Result checksum"):
                matrix.summarize(output)


if __name__ == "__main__":
    unittest.main()
