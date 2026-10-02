# Infra smoke test: brokers, quorum, topics/ISR, ACL enforcement, round-trips.
# Usage (from repo root):  powershell -ExecutionPolicy Bypass -File .\smoke-test.ps1
# See docs/infra.md §9.

$ErrorActionPreference = 'Continue'
$script:failures = 0
$BS = 'kafka-1:9092,kafka-2:9092,kafka-3:9092'
$BIN = '/opt/kafka/bin'
$CC = '/mnt/client-configs'

function Pass($msg) { Write-Host "  PASS  $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  FAIL  $msg" -ForegroundColor Red; $script:failures++ }

# Runs a bash command inside kafka-1; stderr is merged inside the container so PowerShell 5.1
# does not wrap it in ErrorRecords. Avoid double quotes inside $cmd (PS 5.1 native-arg quoting).
function KafkaExec([string]$cmd) {
    $out = docker exec kafka-1 bash -c "$cmd 2>&1"
    return ($out -join "`n")
}

Write-Host "`n[1] Containers"
foreach ($b in 'kafka-1', 'kafka-2', 'kafka-3') {
    $h = docker inspect -f '{{.State.Health.Status}}' $b
    if ($h -eq 'healthy') { Pass "$b healthy" } else { Fail "$b is '$h'" }
}
$init = docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' kafka-init
if ($init -eq 'exited 0') { Pass 'kafka-init completed' } else { Fail "kafka-init: $init" }

Write-Host "`n[2] Controller quorum"
$q = KafkaExec "$BIN/kafka-metadata-quorum.sh --bootstrap-server $BS --command-config $CC/admin.properties describe --status"
if ($q -match 'CurrentVoters:\s*\[.*1.*2.*3.*\]' -or $q -match 'CurrentVoters:.*"id":\s*3') { Pass 'quorum has 3 voters' } else { Fail "quorum:`n$q" }
if ($q -match 'LeaderId:\s*(\d)') { Pass "controller leader = node $($Matches[1])" }

Write-Host "`n[3] Topics and ISR"
$d = KafkaExec "$BIN/kafka-topics.sh --bootstrap-server $BS --command-config $CC/admin.properties --describe"
foreach ($t in @{ 'web.messages' = 3; 'orders' = 3; 'orders.retry' = 3; 'orders.dlq' = 1 }.GetEnumerator()) {
    $parts = ($d -split "`n") | Where-Object { $_ -match "Topic:\s+$([regex]::Escape($t.Key))\s+Partition:" }
    $full = $parts | Where-Object { $_ -match 'Isr:\s+\d+,\d+,\d+' }
    if ($parts.Count -eq $t.Value -and $full.Count -eq $t.Value) { Pass "$($t.Key): $($t.Value) partitions, ISR 3/3" }
    else { Fail "$($t.Key): partitions=$($parts.Count) fullIsr=$($full.Count) expected=$($t.Value)" }
}

Write-Host "`n[4] ACL enforcement"
$r = KafkaExec "echo hacked | timeout 30 $BIN/kafka-console-producer.sh --bootstrap-server $BS --producer.config $CC/intruder.properties --topic orders"
if ($r -match 'TopicAuthorizationException|Not authorized') { Pass 'intruder -> orders denied' } else { Fail "intruder was NOT denied:`n$r" }

$r = KafkaExec "echo cross | timeout 30 $BIN/kafka-console-producer.sh --bootstrap-server $BS --producer.config $CC/py-producer.properties --topic web.messages"
if ($r -match 'TopicAuthorizationException|Not authorized') { Pass 'py-producer -> web.messages denied (least privilege)' } else { Fail "py-producer cross-topic write was NOT denied:`n$r" }

Write-Host "`n[5] Round-trips"
function RoundTrip([string]$user, [string]$topic, [string]$group) {
    $token = "smoke-$user-$([guid]::NewGuid().ToString('N').Substring(0,8))"
    $p = KafkaExec "echo $token | $BIN/kafka-console-producer.sh --bootstrap-server $BS --producer.config $CC/$user.properties --topic $topic --producer-property acks=all"
    if ($p -match 'Exception|ERROR') { Fail "$user produce to ${topic}:`n$p"; return }
    $c = KafkaExec "$BIN/kafka-console-consumer.sh --bootstrap-server $BS --consumer.config $CC/$user.properties --topic $topic --group $group --from-beginning --timeout-ms 15000"
    if ($c -match $token) { Pass "$user produce+consume on $topic (group $group)" } else { Fail "$user did not read back $token on ${topic}:`n$c" }
}
RoundTrip 'web-app' 'web.messages' 'web-consumer-smoke'

# No write test on orders: junk records there would later reach the Python consumer.
$c = KafkaExec "$BIN/kafka-console-consumer.sh --bootstrap-server $BS --consumer.config $CC/py-consumer.properties --topic orders --group not-allowed-group --timeout-ms 8000"
if ($c -match 'GroupAuthorizationException|Not authorized') { Pass 'py-consumer with unknown group denied' } else { Fail "py-consumer unknown group was NOT denied:`n$c" }

Write-Host ''
if ($script:failures -eq 0) { Write-Host 'ALL CHECKS PASSED' -ForegroundColor Green; exit 0 }
else { Write-Host "$script:failures CHECK(S) FAILED" -ForegroundColor Red; exit 1 }
