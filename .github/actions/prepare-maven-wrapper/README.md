<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Maven Wrapper Preparation

Run this composite action after checkout and before the first `mvnw` or
`mvnw.cmd` invocation in a backend job. It requires Bash, curl and either
sha256sum or shasum on the CI runner, not on developers' local wrapper paths.

Only `.mvn/wrapper/maven-wrapper.jar` is cached. The exact key includes the
runner OS, wrapper properties and preparation script. Restored and downloaded
bytes must match `wrapperSha256Sum` in the checked-in properties. Review and
update that pin from the official artifact when changing the wrapper version;
the action never downloads a checksum at runtime. An invalid cached JAR is
removed and fetched again. Downloads are renamed into place only after
verification. A verified cache miss is saved immediately, before Maven tests.
GitHub caches are immutable: a corrupt exact-key hit is repaired locally but
requires cache eviction to repair the saved entry.

Downloads allow five attempts, each with a 10-second connection timeout and
15-second total transfer timeout. Retry sleeps are 5, 10, 20 and 40 seconds,
with a cumulative sleep budget of 75 seconds. Numeric Retry-After can increase
a sleep within that budget. HTTP-date, malformed or oversized Retry-After
values fail closed instead of causing an early retry. Permanent HTTP errors,
checksum mismatches and final exhaustion fail the preparation step and prevent
Maven from starting. The transfer-plus-sleep bound is 150 seconds, excluding
local verification and cache service time; existing job timeouts are unchanged.

Neither launcher is modified. Maven distribution downloads, extracted Maven
executables and dependency caching are outside this action's scope.

Run the deterministic offline regression tests from the repository root:

```sh
bash .github/actions/prepare-maven-wrapper/test.sh
```

The fixtures stub curl and sleep and never run Java or Maven. They validate
bootstrap control flow, not live Central availability or GitHub cache behavior.
