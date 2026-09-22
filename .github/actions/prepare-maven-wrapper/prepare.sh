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
fail() {
    echo "Maven wrapper bootstrap failed: $*" >&2
    exit 1
}

properties=.mvn/wrapper/maven-wrapper.properties
jar=.mvn/wrapper/maven-wrapper.jar
url= checksum=
while IFS='=' read -r key value; do
    value=${value%$'\r'}
    case "$key" in
        wrapperUrl) url=$value ;;
        wrapperSha256Sum) checksum=$value ;;
    esac
done < "$properties"
[[ "$url" == https://* ]] || fail 'wrapperUrl must use HTTPS'
[[ "$checksum" =~ ^[0-9a-f]{64}$ ]] || fail 'missing or invalid pinned SHA-256'
if command -v sha256sum >/dev/null; then
    hash_command=(sha256sum)
elif command -v shasum >/dev/null; then
    hash_command=(shasum -a 256)
else
    fail 'SHA-256 verification requires sha256sum or shasum'
fi
verified() {
    local actual
    actual=$("${hash_command[@]}" "$1") || return 1
    [ "${actual%% *}" = "$checksum" ]
}

if [ -f "$jar" ] && verified "$jar"; then
    echo 'Maven wrapper JAR verified (cached)'
    exit 0
fi
rm -f "$jar"
command -v curl >/dev/null || fail 'curl is required on the CI runner'
download=$(mktemp "${jar}.tmp.XXXXXX")
headers="${download}.headers"
trap 'rm -f "$download" "$headers"' EXIT
trap 'exit 1' HUP INT TERM

# At most 5 * 15 seconds of transfer time plus 75 seconds of sleeps.
# Do not let curl retry internally: this loop owns the entire retry budget.
delay=5
sleep_budget=75
for attempt in 1 2 3 4 5; do
    : > "$download"
    : > "$headers"
    status=0
    http_code=$(curl --disable --silent --show-error --fail --location \
        --proto '=https' --proto-redir '=https' \
        --connect-timeout 10 --max-time 15 \
        --dump-header "$headers" --output "$download" --write-out '%{http_code}' \
        "$url") || status=$?
    if [ "$status" = 0 ] && [ "$http_code" = 200 ]; then
        verified "$download" || fail 'downloaded JAR does not match the pinned SHA-256'
        mv -f "$download" "$jar"
        echo 'Maven wrapper JAR downloaded and verified'
        exit 0
    fi
    # A partial response is never retained as an executable/cache candidate.
    : > "$download"
    case "$status:$http_code" in
        22:408|22:429|22:500|22:502|22:503|22:504|5:*|6:*|7:*|18:*|28:*|52:*|56:*) ;;
        *) fail "download failed (curl $status, HTTP $http_code); not retryable" ;;
    esac
    [ "$attempt" -lt 5 ] || fail "download exhausted 5 attempts (curl $status, HTTP $http_code)"
    retry_after=$(awk '
        /^HTTP\// { value = "" }
        tolower($1) == "retry-after:" { sub(/^[^:]*:[[:space:]]*/, ""); sub(/\r$/, ""); value = $0 }
        END { print value }
    ' "$headers")
    wait_seconds=$delay
    if [ -n "$retry_after" ]; then
        # Fail closed on HTTP dates or oversized hints rather than retrying early.
        [[ "$retry_after" =~ ^[0-9]{1,2}$ ]] || fail 'unsupported Retry-After; no early retry'
        retry_after=$((10#$retry_after))
        if [ "$retry_after" -gt "$wait_seconds" ]; then
            wait_seconds=$retry_after
        fi
    fi
    [ "$wait_seconds" -le "$sleep_budget" ] || fail 'Retry-After/backoff exceeds remaining sleep budget'
    echo "Maven wrapper download failed (curl $status, HTTP $http_code); attempt $attempt/5, retrying in ${wait_seconds}s" >&2
    sleep "$wait_seconds"
    sleep_budget=$((sleep_budget - wait_seconds))
    delay=$((delay * 2))
done
