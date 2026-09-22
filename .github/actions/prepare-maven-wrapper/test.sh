#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail
action_dir=$(cd "$(dirname "$0")" && pwd)
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT
mkdir -p "$fixture/bin" "$fixture/project/.mvn/wrapper"
printf 'verified fixture bytes\n' > "$fixture/expected.jar"
if command -v sha256sum >/dev/null; then
    checksum=$(sha256sum "$fixture/expected.jar")
else
    checksum=$(shasum -a 256 "$fixture/expected.jar")
fi
checksum=${checksum%% *}
cat > "$fixture/project/.mvn/wrapper/maven-wrapper.properties" <<EOF
wrapperUrl=https://repo.maven.apache.org/maven2/org/apache/maven/wrapper/maven-wrapper/3.1.0/maven-wrapper-3.1.0.jar
wrapperSha256Sum=$checksum
EOF
cat > "$fixture/bin/curl" <<'EOF'
#!/usr/bin/env bash
set -eu
[ "$1" = --disable ]
printf 'call\n' >> "$FIXTURE/calls"
output= headers=
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output) output=$2; shift ;;
        --dump-header) headers=$2; shift ;;
        --max-time) [ "$2" = 15 ] || exit 99; shift ;;
        --connect-timeout) [ "$2" = 10 ] || exit 99; shift ;;
        --proto|--proto-redir) [ "$2" = '=https' ] || exit 99; shift ;;
    esac
    shift
done
[ -n "$output" ] && [ -n "$headers" ]
# Downloads must not replace the final jar before verification.
[ "$output" != '.mvn/wrapper/maven-wrapper.jar' ] || exit 99
count=$(wc -l < "$FIXTURE/calls" | tr -d ' ')
status=$(sed -n "${count}p" "$FIXTURE/responses")
[ -n "$status" ] || exit 99
printf 'HTTP/1.1 %s\r\n' "$status" > "$headers"
if [ -n "${RETRY_AFTER:-}" ]; then
    printf 'Retry-After: %s\r\n' "$RETRY_AFTER" >> "$headers"
fi
printf '\r\n' >> "$headers"
printf '%s' "$status"
if [ "$status" != 200 ]; then
    printf partial > "$output"
    [ "$status" != 000 ] || exit 28
    exit 22
fi
if [ "${BAD_DOWNLOAD:-false}" = true ]; then
    printf corrupt > "$output"
else
    cp "$FIXTURE/expected.jar" "$output"
fi
EOF
cat > "$fixture/bin/sleep" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$1" >> "$FIXTURE/sleeps"
EOF
chmod +x "$fixture/bin/curl" "$fixture/bin/sleep"
export FIXTURE="$fixture" PATH="$fixture/bin:$PATH"
cd "$fixture/project"
jar=.mvn/wrapper/maven-wrapper.jar

reset_case() {
    rm -f "$jar" "$fixture/launched"
    : > "$fixture/calls"
    : > "$fixture/sleeps"
    unset RETRY_AFTER BAD_DOWNLOAD
    printf '%s\n' "$@" > "$fixture/responses"
}

run_case() {
    # Model sequential Actions steps: a failed preparation must stop before mvnw.
    if bash "$action_dir/prepare.sh" > "$fixture/output" 2>&1; then
        touch "$fixture/launched"
        result=0
    else
        result=$?
    fi
}

expect_calls() {
    [ "$(wc -l < "$fixture/calls" | tr -d ' ')" = "$1" ]
}

expect_success() {
    [ "$result" = 0 ]
    [ -f "$fixture/launched" ]
    cmp "$jar" "$fixture/expected.jar"
    [ -z "$(find .mvn/wrapper -name '*.tmp.*' -print)" ]
}

expect_failure() {
    [ "$result" != 0 ]
    [ ! -e "$fixture/launched" ]
    [ ! -e "$jar" ]
    [ -z "$(find .mvn/wrapper -name '*.tmp.*' -print)" ]
    grep -q 'Maven wrapper bootstrap failed:' "$fixture/output"
}

reset_case
cp "$fixture/expected.jar" "$jar"
run_case
expect_success
expect_calls 0
echo 'PASS verified cache: no HTTP requests'

reset_case 429 429 200
run_case
expect_success
expect_calls 3
[ "$(tr '\n' ' ' < "$fixture/sleeps")" = '5 10 ' ]
echo 'PASS 429 recovery: bounded exponential backoff'

reset_case 000 200
run_case
expect_success
expect_calls 2
echo 'PASS partial transfer timeout: verified recovery'

reset_case 429 429 429 429 429
printf partial > "$jar"
run_case
expect_failure
expect_calls 5
[ "$(tr '\n' ' ' < "$fixture/sleeps")" = '5 10 20 40 ' ]
echo 'PASS exhausted 429: clear failure, no launcher, no partial jar'

for contents in corrupt partial ''; do
    reset_case 200
    printf '%s' "$contents" > "$jar"
    run_case
    expect_success
    expect_calls 1
done
echo 'PASS corrupt, partial and empty caches: replaced only after verification'

reset_case 200
export BAD_DOWNLOAD=true
run_case
expect_failure
expect_calls 1
echo 'PASS bad downloaded checksum: no launcher or retries'

reset_case 404
run_case
expect_failure
expect_calls 1
[ ! -s "$fixture/sleeps" ]
echo 'PASS permanent HTTP error: no retries'

reset_case 429 200
export RETRY_AFTER=12
run_case
expect_success
[ "$(cat "$fixture/sleeps")" = 12 ]
echo 'PASS numeric Retry-After honored'

for retry_after in 76 'Wed, 23 Sep 2026 00:00:00 GMT' invalid 999999999999999999999999999; do
    reset_case 429
    export RETRY_AFTER="$retry_after"
    run_case
    expect_failure
    expect_calls 1
    [ ! -s "$fixture/sleeps" ]
done
echo 'PASS excessive or unsupported Retry-After: fail instead of retrying early'

reset_case 429 429
export RETRY_AFTER=40
run_case
expect_failure
expect_calls 2
[ "$(cat "$fixture/sleeps")" = 40 ]
echo 'PASS cumulative retry sleep budget'

reset_case
cp "$fixture/expected.jar" "$jar"
sed '/wrapperSha256Sum=/d' .mvn/wrapper/maven-wrapper.properties > properties.tmp
mv properties.tmp .mvn/wrapper/maven-wrapper.properties
run_case
[ "$result" != 0 ]
[ ! -e "$fixture/launched" ]
expect_calls 0
echo 'PASS missing checksum fails closed even with a readable cache'
