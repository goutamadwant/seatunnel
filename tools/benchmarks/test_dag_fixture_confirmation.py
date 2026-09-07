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

import copy
import pathlib
import tempfile
import unittest
import zipfile

from dag_fixture_confirmation import ALLOWED_CLASSES, FLAGS, check_classes, check_version_metadata, plan, validate


class ConfirmationTest(unittest.TestCase):
    def versions(self):
        old = {'project.version': '3.0.0-SNAPSHOT', 'git.commit.id': 'a' * 40,
               'git.commit.id.abbrev': 'a' * 7, 'git.commit.time': '2026-09-01T00:00:00+0000',
               'git.build.time': '2026-09-07T00:00:00+0000'}
        new = dict(old, **{'git.commit.id': 'b' * 40, 'git.commit.id.abbrev': 'b' * 7,
                         'git.build.time': '2026-09-07T01:00:00+0000'})
        return old, new

    def test_only_verified_provenance_metadata_may_differ(self):
        old, new = self.versions()
        check_version_metadata(old, new, {'baseline': 'a' * 40, 'candidate': 'b' * 40})
        for field, value in [('project.version', '4.0.0'), ('git.commit.id', 'c' * 40),
                             ('git.commit.id.abbrev', 'c' * 7), ('extra.setting', 'true')]:
            changed = dict(new, **{field: value})
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                check_version_metadata(old, changed, {'baseline': 'a' * 40, 'candidate': 'b' * 40})

    def test_packaged_provenance_does_not_weaken_class_guard(self):
        with tempfile.TemporaryDirectory() as directory:
            old, new = [pathlib.Path(directory) / name for name in ['old.jar', 'new.jar']]
            fixture = sorted(ALLOWED_CLASSES)[0]
            for jar, props in zip([old, new], self.versions()):
                with zipfile.ZipFile(jar, 'w') as archive:
                    archive.writestr(fixture, jar.name)
                    archive.writestr('zeta.version.properties', '\n'.join(k + '=' + v for k, v in props.items()))
            changed = check_classes(old, new, {'baseline': 'a' * 40, 'candidate': 'b' * 40})
            self.assertEqual(set(changed), {fixture, 'zeta.version.properties'})

    def record(self):
        return {'benchmark': 'org.apache.seatunnel.benchmark.IMapDagStorageBenchmark.finishedJobDagStore',
                'jmhVersion': '1.37', 'mode': 'ss', 'forks': 3, 'threads': 1,
                'warmupIterations': 3, 'measurementIterations': 5,
                'warmupBatchSize': 1, 'measurementBatchSize': 1, 'jvmArgs': FLAGS,
                'params': {'pipelineCount': '1', 'storedDagCount': '100'},
                'primaryMetric': {'scoreUnit': 'us/op', 'score': 1.0, 'rawData': [[1.0] * 5 for _ in range(3)]}}

    def test_original_protocol_and_counterbalance(self):
        self.assertEqual(len(list(plan('11'))), 8)
        self.assertEqual(len(list(plan('8'))), 4)
        self.assertEqual([row[2] for row in plan('11')],
                         ['baseline', 'candidate', 'candidate', 'baseline',
                          'candidate', 'baseline', 'baseline', 'candidate'])
        self.assertEqual(len(validate(self.record(), 1, 100)), 3)

    def test_changed_measurement_contract_is_rejected(self):
        for key, value in [('mode', 'avgt'), ('forks', 1), ('measurementIterations', 10),
                           ('warmupIterations', 10), ('jvmArgs', []), ('params', {})]:
            with self.subTest(key=key):
                record = self.record()
                record[key] = value
                with self.assertRaises(RuntimeError):
                    validate(record, 1, 100)

    def test_incomplete_nonfinite_or_misnormalized_results_rejected(self):
        original = self.record()
        for metric in [{'rawData': [[1.0] * 5]}, {'scoreUnit': 'ms/op'}, {'score': 100.0},
                       {'rawData': [[float('nan')] * 5 for _ in range(3)]}]:
            record = copy.deepcopy(original)
            record['primaryMetric'].update(metric)
            with self.assertRaises(RuntimeError):
                validate(record, 1, 100)

    def test_only_fixture_classes_may_change(self):
        with tempfile.TemporaryDirectory() as directory:
            old = pathlib.Path(directory) / 'old.jar'
            new = pathlib.Path(directory) / 'new.jar'
            fixture = sorted(ALLOWED_CLASSES)[0]
            def write(path, fixture_bytes, production_bytes):
                with zipfile.ZipFile(path, 'w') as archive:
                    archive.writestr(fixture, fixture_bytes)
                    archive.writestr('Production.class', production_bytes)
            write(old, b'old', b'production')
            write(new, b'new', b'production')
            self.assertEqual(check_classes(old, new), [fixture])
            write(new, b'new', b'changed')
            with self.assertRaises(RuntimeError):
                check_classes(old, new)
            write(new, b'old', b'production')
            with self.assertRaises(RuntimeError):
                check_classes(old, new)


if __name__ == '__main__':
    unittest.main()
