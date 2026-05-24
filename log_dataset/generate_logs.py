import json
import random
from datetime import datetime, timedelta

random.seed(42)

SERVICES = ["payment-service", "auth-service", "kafka-consumer", "spark-job", "data-mart-loader", "api-gateway", "redis-cache"]
LEVELS_NORMAL = ["INFO", "WARN"]
LEVELS_ANOMALY = ["ERROR", "FATAL", "WARN"]
ANOMALY_TYPES = ["OOM", "deadlock", "connection_timeout", "cascade_failure", "auth_error", "data_corruption"]
TABLES = ["fact_sales", "dim_users", "dim_accounts", "payment_transactions", "audit_log", "session_store", "fraud_events", "customer_profile", "merchant_settlements", "risk_score_results"]

def render(s, **kw):
    try:
        return s.format(**kw)
    except KeyError:
        return s

NORMAL_MSGS = [
    "Payment processed successfully for order {n}",
    "Запрос авторизации подтвержден, user_id={n}",
    "Kafka message consumed from topic payments, offset {n}",
    "Spark job stage {n} completed successfully",
    "Data mart loader: table {table} updated, {n} rows affected",
    "API request {method} {path} returned 200 in {n}ms",
    "Redis cache hit for key session:{n}",
    "Deploy v{n} rolled out to production successfully",
    "Health check passed for {service} node {n}, service is healthy",
    "Connection pool refreshed, {n} connections acquired",
    "Batch job scheduler: task {n} completed at {time}",
    "Metric: p99 latency={n}ms, within SLO",
    "Scheduled backup completed, size={n}MB",
    "User {n} logged in successfully from IP 10.0.{a}.{b}",
    "Database migration v{n} applied, no errors",
    "Heartbeat received from {service} at {time}",
    "Rate limit check passed for API key #{n}",
    "Cache warmup finished, {n} keys loaded",
    "Transaction #{n} committed in {a}ms",
    "SSL certificate renewed for {host}, expires {date}",
    "Connection timeout during retry — reconnecting to {service} node {n} (normal)",
    "Соединение разорвано, выполняю штатный реконнект к {service} (attempt {n})",
    "Read timeout on attempt {n}, initiating retry with backoff (normal)",
    "Connection pool exhausted for {service}, waiting for release (normal condition, {n} active)",
    "Retry #{n} for message {id}: temporary failure, will retry (normal)",
    "Load balancer health check: {service} node {n} is UP",
    "Circuit breaker closed for {service} node {n}, normal operation",
    "Leader election completed, new leader = {service}-{n}",
    "Disk usage at {n}%, within threshold",
    "Replica lag = {n}ms, acceptable",
    "Config reload triggered by {user} on {service}, no errors, version v{n}",
    "Graceful shutdown of {service} node {n} initiated for maintenance",
    "Warm restart of {service} completed in {n}ms",
    "Schema registry: schema v{n} registered for topic {topic}",
    "Thread pool active count = {n}, queue depth = {a}, normal operation",
    "Таймаут соединения при попытке {n}, повтор через {a}ms (штатный сценарий)",
    "Deadlock detector: no deadlocks found in current cycle for {service}, checked {n} locks",
    "OOM prevention: heap usage {n}%, GC cycles normal",
    "Connection refused on port {n}, expected during rolling restart",
    "DNS resolution for {host} succeeded, TTL={n}s",
    "Pod {service}-{n} readiness probe passed",
    "Backpressure signal relieved in {service}, processing resumed at offset {n}",
    "Shard rebalancing in {service}: {n} partitions moved, {a} remaining",
    "Compaction completed for topic {topic}, {n} segments merged",
    "Checkpoint saved at offset {n} for consumer group {service}",
]

ANOMALY_MSGS = {
    "OOM": [
        "java.lang.OutOfMemoryError: Java heap space in {service}",
        "OutOfMemoryError: unable to create new native thread in {service}",
        "Kubernetes OOMKilled: pod {service}-{n} exceeded memory limit",
        "GC overhead limit exceeded — heap usage {n}% in {service}",
        "Native memory allocation (mmap) failed for {n}MB in {service}",
        "Metaspace overflow: class loader leak detected in {service}",
        "Direct buffer memory exhausted in {service}, restart required",
        "OOM: Compressed class space exhausted in {service}",
        "Container memory limit {n}MB reached, OOM killer triggered",
        "Unable to allocate {n}MB for query execution in {service}",
        "Heap dump triggered: {service} heap = {n}GB, OOM imminent",
        "OOM in Spark executor {n}, stage {a}, task {b}",
        "GC allocation failure in {service}, promoting to full GC",
        "JVM crash: OutOfMemoryError in {service} thread pool",
        "Memory pressure critical in {service}: {n}MB / {a}MB used",
        "OOM prevented by circuit breaker, dropping requests to {service}",
        "Swap usage {n}% in {service}, OOM risk elevated",
        "Physical memory exhausted on node {host}, killing {service}",
        "OOM: stack trace {service}.processRequest — cannot allocate {n} bytes",
        "Hazelcast OOM: {service} backup sync failed, data may be lost",
        "OOM error in {service} — proceeding with emergency restart",
        "Python MemoryError in data pipeline {service}, batch {n} failed",
        "InfluxDB OOM: shard {n} cannot be loaded in {service}",
    ],
    "deadlock": [
        "Deadlock detected in {service}: transaction {n} and {a} are mutually waiting",
        "Deadlock: thread-{n} locked resource A, thread-{a} locked resource B, circular wait",
        "ERROR: deadlock detected — restarting transaction {n} in {service}",
        "InnoDB deadlock: {service} transaction {n} rolled back",
        "Deadlock in connection pool: {service} threads {n}..{a} blocked",
        "Cascading lock wait timeout in {service}: transaction {n} blocked by {a}",
        "Deadlock detector found cycle: {service} locks #{n}, #{a}, #{b}",
        "Database deadlock: {service} query crossed with {a}",
        "ZooKeeper deadlock: {service} session {n} stuck on leader election",
        "Deadlock in distributed commit: {service} 2PC phase-1 timeout",
        "Thread dump: {service} threads {n}..{a} in BLOCKED state (deadlock)",
        "Deadlock in Hazelcast: {service} lock key={n} not released after {a}s",
        "Transaction deadlock: {service} — all retries ({n}) exhausted",
        "Deadlock between {service} and {service}: cross-service lock conflict",
        "Deadlock in Kafka consumer group {n}: rebalance stuck",
        "Deadlock prevention: {service} aborted transaction {n} after {a}ms wait",
        "Lock escalation deadlock in {service}: page lock -> table lock -> deadlock",
        "Database session {n} in {service}: waiting for lock held by session {a}",
        "Deadlock in Spark driver {n}: task scheduler blocked",
        "Postgres deadlock: {service} query {n} — select for update vs update conflict",
        "Redis deadlock: {service} Lua script {n} held lock longer than timeout",
        "Quartz scheduler deadlock in {service}: job {n} and {a} circular dependency",
    ],
    "connection_timeout": [
        "Connection timeout to database {service} after {n}s",
        "Connection timeout to Kafka broker {n}.{a}.{b}.{n}:9092",
        "Redis connection timeout after {n}ms, cluster may be degraded",
        "Connect timeout to {service} after {n} attempts — service unreachable",
        "Connection timeout: socket read timed out in {service}",
        "Connection timeout to external gateway {host}, downstream may be down",
        "Connection timeout during TLS handshake with {service}",
        "JDBC connection timeout in {service}: pool exhausted, wait timeout {n}ms",
        "Connection timeout on ZooKeeper ensemble — leader unreachable",
        "HTTP connection timeout to {service}/health after {n}ms",
        "gRPC connection timeout to {service}: deadline exceeded {n}s",
        "Cassandra connection timeout in {service}: no host available",
        "Connection timeout in {service} data replica sync: {host} not responding",
        "Elasticsearch connection timeout: cluster {n} unreachable after {a}ms",
        "RabbitMQ connection timeout in {service}: channel {n} blocked",
        "SSH connection timeout to {host} for {service} maintenance task",
        "LDAP connection timeout in {service}: auth backend {host} not responding",
        "S3 connection timeout: {service} unable to access bucket {n} after {a}s",
        "Connection timeout in spark shuffle service {n}.{a}: fetch failed",
        "Thrift connection timeout to {service} after {n}ms, retrying node {a}",
        "MongoDB connection timeout: {service} replica set primary not reachable",
        "NATS connection timeout in {service}: cluster {n} route disconnected",
        "Соединение разорвано: {service} не отвечает после {n} попыток",
    ],
    "cascade_failure": [
        "Cascade failure detected: {service} failure propagating to {a} downstream services",
        "Circuit breaker opened in {service}: failures = {n}/{a}, cascading to {b} dependents",
        "Cascade failure: {service} latency spike caused timeouts in multiple downstream services",
        "Bulkhead rejected request in {service}: thread pool {n} saturated, cascade to {a}",
        "Error propagation from {service}: {n} clients received 5xx in {a}ms window",
        "Cascade: {service} crashed -> load shifted to {a} -> {a} also overloaded",
        "Retry storm cascade: {service} retries ({n}/s) overwhelming downstream {a}",
        "Cascade failure in Spark DAG: stage {n} failed, downstream stages cancelled",
        "Cache avalanche cascade: Redis {service} down -> {n} requests/s hitting database directly",
        "DNS cascade failure: {service} resolution failed -> {n} dependent services degraded",
        "Coordinated omission cascade: {service} latency {n}ms caused queue buildup in {a}",
        "Cascade failure: {service} partial outage -> {n}% of traffic routed to degraded node",
        "Dead letter cascade: Kafka consumer {service} fell behind by {n} messages",
        "Resource cascade: {service} exhausted file descriptors -> fork failed -> {a} crashed",
        "Cross-AZ cascade: {service} in AZ-{n} failed, AZ-{a} cannot handle full load",
        "Cascade failure in microservices mesh: {service} -> {a} -> {b} in domino effect",
        "Config push cascade: bad config in {service} caused {n} reloads",
        "Connection pool cascade: {service} pool exhaustion -> timeout -> retry storm",
        "Cascade failure: {service} leader crash -> re-election -> {n} follower timeouts",
        "Rate limiter cascade: {service} blocked -> clients retry harder -> all upstream throttled",
        "Memory leak cascade: {service} consumed all RAM -> OOM -> systemd restarted",
        "ZooKeeper cascade: {service} session expiry -> ephemeral nodes vanished -> {n} lost config",
    ],
    "auth_error": [
        "Authentication failed for user {n} on {service}: invalid credentials",
        "JWT token expired in {service}: user {n} session invalidated",
        "OAuth2 token validation failed in {service}: invalid signature",
        "SSL handshake failed in {service}: certificate expired for {host}",
        "Authorization denied: user {n} lacks permission {a} on {service}",
        "LDAP bind failed in {service}: account {n} locked after {a} failed attempts",
        "SAML assertion validation failed in {service}: audience mismatch",
        "MFA challenge failed for user {n} in {service}: invalid TOTP code",
        "Kerberos ticket expired in {service}: renew required for {host}",
        "API key validation failed in {service}: key {n} revoked",
        "Session token compromised: {service} forced logout for all sessions of user {n}",
        "ACL violation in {service}: topic {topic} write denied for consumer {n}",
        "CSRF token validation failed in {service}: request rejected",
        "OpenID Connect discovery failed in {service}: issuer {host} not trusted",
        "Certificate revocation check failed in {service}: OCSP responder unreachable",
        "SASL authentication failed in {service}: mechanism {a} not supported by broker {n}",
        "Password rotation required: {service} — user {n} password exceeds max age",
        "SSO token mismatch in {service}: state parameter validation failed",
        "RBAC policy violation in {service}: role {n} missing permission {a}",
        "Service account {n} credential rotation overdue by {a} days in {service}",
        "OAuth2 client authentication failed: {service} — client_id {n} not registered",
        "IAM role assumption failed in {service}: trust policy for {host} rejected",
    ],
    "data_corruption": [
        "Checksum mismatch in {service}: expected {n}, got {a} for partition {b}",
        "Data corruption detected in {service}: invalid record at offset {n} in topic {topic}",
        "Page corruption in {service}: database page {n} checksum failed",
        "Parquet file corruption in {service}: column chunk CRC mismatch for {table}",
        "JSON parsing error in {service}: malformed message at offset {n}",
        "Index corruption in {service}: table {table} index {n} rebuild required",
        "WAL corruption in {service}: last valid LSN = {n}, recovery needed",
        "Avro schema mismatch in {service}: incompatible schema evolution for topic {topic}",
        "Redis RDB corruption: {service} failed to load dump.rdb",
        "Protobuf deserialization failed in {service}: wire type mismatch in field {n}",
        "Bit rot detected in {service}: file {path} has silent data corruption",
        "InnoDB page corruption in {service}: tablespace {table} corrupted at page {n}",
        "Kafka log corruption in {service}: segment {n} has invalid CRC",
        "Data skew corruption in {service}: join key {n} produced NULL in non-nullable column {a}",
        "Sequence number gap in {service}: expected {n}, next is {a} — possible data loss",
        "Corrupted message in DLQ: {service} — unable to parse {n} bytes at offset {a}",
        "HBase region corruption in {service}: region {n} needs reassignment",
        "Cassandra SSTable corruption: {service} — {table} repair needed for token range {n}",
        "ORC file corruption in {service}: stripe {n} footer checksum error in {table}",
        "Duplicate primary key detected in {service}: table {table}, key {n} conflicts",
        "Referential integrity violation in {service}: foreign key {n} has no matching parent in {table}",
        "Elasticsearch index corruption: {service} shard {n} of {table} has corrupted lucene files",
    ],
}

TRICKY_PAIRS = [
    ("Connection timeout during retry — reconnecting to {service} node {n} (normal)", "normal"),
    ("Connection timeout to database {service} after {n}s", "anomaly"),
    ("Соединение разорвано, выполняю штатный реконнект к {service} (attempt {n})", "normal"),
    ("Соединение разорвано: {service} не отвечает после {n} попыток", "anomaly"),
    ("Read timeout on attempt {n}, initiating retry with backoff (normal)", "normal"),
    ("Connection timeout: socket read timed out in {service}", "anomaly"),
    ("Connection pool exhausted for {service}, waiting for release (normal condition, {n} active)", "normal"),
    ("Connection pool exhausted, threads blocked indefinitely in {service}", "anomaly"),
    ("Retry #{n} for message {id}: temporary failure, will retry (normal)", "normal"),
    ("Retry storm cascade: {service} retries ({n}/s) overwhelming downstream {a}", "anomaly"),
    ("Deadlock detector: no deadlocks found in current cycle for {service}, checked {n} locks", "normal"),
    ("Deadlock detected in {service}: transaction {n} and {a} are mutually waiting", "anomaly"),
    ("OOM prevention: heap usage {n}%, GC cycles normal", "normal"),
    ("java.lang.OutOfMemoryError: Java heap space in {service}", "anomaly"),
    ("Rate limit check passed for API key #{n}", "normal"),
    ("Rate limiter cascade: {service} blocked -> clients retry harder -> all upstream throttled", "anomaly"),
    ("Graceful shutdown of {service} node {n} initiated for maintenance", "normal"),
    ("Cascade: {service} crashed -> load shifted to {a} -> {a} also overloaded", "anomaly"),
    ("Warm restart of {service} completed in {n}ms", "normal"),
    ("Config push cascade: bad config in {service} caused {n} reloads", "anomaly"),
]

def generate_batch(batch_num, id_start, id_end):
    size = id_end - id_start + 1
    NORMAL_CT = 140
    ANOMALY_CT = 60

    # Build unique message pool for this batch
    used = set()

    def make_msg(template, n, a=1, b=1):
        for attempt in range(100):
            service = random.choice(SERVICES)
            table = random.choice(TABLES)
            method = random.choice(["GET", "POST", "PUT", "DELETE"])
            path = random.choice(["/api/v1/payments", "/api/v1/auth/login", "/api/v1/orders", "/api/v1/users", "/api/v1/reports"])
            host = f"host-{random.randint(0,49)}.internal"
            topic = random.choice(["payments", "orders", "events", "audit"])
            user = random.choice(["admin", "svc-account", "batch-user"])
            now = datetime(2024, 3, 15, 8, 0, 0) + timedelta(seconds=n * 7 + attempt * 13 % 86400)
            ts = now.strftime("%Y-%m-%d %H:%M:%S")
            msg = render(template, n=n, a=a, b=b, service=service, table=table,
                         method=method, path=path, host=host, topic=topic, user=user,
                         time=ts, date=now.strftime("%Y-%m-%d"), id=n)
            if msg not in used:
                used.add(msg)
                return msg, service, ts
        msg = render(template, n=n, a=id_start + random.randint(0,199), a2=random.randint(1,50),
                     service=random.choice(SERVICES), table=random.choice(TABLES),
                     method="GET", path="/api", host="host-0.internal",
                     topic="payments", user="admin",
                     time=now.strftime("%Y-%m-%d %H:%M:%S"), date="2024-03-15", id=n)
        used.add(msg)
        return msg, random.choice(SERVICES), now.strftime("%Y-%m-%d %H:%M:%S")

    records = []
    ids = list(range(id_start, id_end + 1))
    random.shuffle(ids)
    id_iter = iter(ids)

    # Assign tricky entries
    tricky_pool = list(TRICKY_PAIRS)
    random.shuffle(tricky_pool)
    tricky_assignments = []  # (template, label) for this batch (at least 10)
    nt = min(len(tricky_pool), 10)
    for i in range(nt):
        tricky_assignments.append(tricky_pool[i])

    # Count how many normal vs anomaly tricky
    tricky_normal = sum(1 for _, lbl in tricky_assignments if lbl == "normal")
    tricky_anomaly = nt - tricky_normal

    normal_needed = NORMAL_CT - tricky_normal
    anomaly_needed = ANOMALY_CT - tricky_anomaly

    # Build anomaly entries for the batch
    at_list = []
    for at in ANOMALY_TYPES:
        pool = list(ANOMALY_MSGS[at])
        random.shuffle(pool)
        for i in range(10):
            at_list.append((pool[i % len(pool)], at))
    random.shuffle(at_list)

    # Process tricky entries first
    for msg_tmpl, label in tricky_assignments:
        rid = next(id_iter)
        n_val = rid
        a_val = random.randint(1, 50)
        b_val = random.randint(1, 20)

        if label == "normal":
            at = "none"
            level = "WARN"
        else:
            # Determine anomaly type from template content
            if "deadlock" in msg_tmpl.lower():
                at = "deadlock"
            elif "oom" in msg_tmpl.lower() or "memory" in msg_tmpl.lower():
                at = "OOM"
            elif "cascade" in msg_tmpl.lower() or "storm" in msg_tmpl.lower():
                at = "cascade_failure"
            elif "timeout" in msg_tmpl.lower() or "retry" in msg_tmpl.lower() or "соединен" in msg_tmpl.lower() or "разорван" in msg_tmpl.lower():
                at = "connection_timeout"
            elif "rate" in msg_tmpl.lower():
                at = "cascade_failure"
            else:
                at = "connection_timeout"
            level = "ERROR"

        msg, svc, ts = make_msg(msg_tmpl, n=n_val, a=a_val, b=b_val)
        records.append({
            "id": rid,
            "timestamp": ts,
            "service": svc,
            "level": level,
            "message": msg,
            "label": label,
            "anomaly_type": at if label == "anomaly" else "none",
        })

    # Fill remaining normal
    normal_pool = list(NORMAL_MSGS)
    random.shuffle(normal_pool)
    ni = 0
    while normal_needed > 0:
        rid = next(id_iter)
        tmpl = normal_pool[ni % len(normal_pool)]
        ni += 1
        msg, svc, ts = make_msg(tmpl, n=rid, a=random.randint(1,50), b=random.randint(1,20))
        records.append({
            "id": rid, "timestamp": ts, "service": svc,
            "level": random.choice(LEVELS_NORMAL),
            "message": msg, "label": "normal", "anomaly_type": "none",
        })
        normal_needed -= 1

    # Fill remaining anomaly
    ai = 0
    while anomaly_needed > 0:
        rid = next(id_iter)
        tmpl, at = at_list[ai % len(at_list)]
        ai += 1
        msg, svc, ts = make_msg(tmpl, n=rid, a=random.randint(1,50), b=random.randint(1,20))
        records.append({
            "id": rid, "timestamp": ts, "service": svc,
            "level": random.choice(LEVELS_ANOMALY),
            "message": msg, "label": "anomaly", "anomaly_type": at,
        })
        anomaly_needed -= 1

    records.sort(key=lambda x: x["id"])
    return records

all_batches = []
for batch_num in range(1, 6):
    id_start = (batch_num - 1) * 200 + 1
    id_end = batch_num * 200
    batch = generate_batch(batch_num, id_start, id_end)
    all_batches.append(batch)
    with open(f"batch_{batch_num}.json", "w") as f:
        json.dump(batch, f, ensure_ascii=False, indent=2)
    label = f"Батч {batch_num} готов"
    if batch_num < 5:
        label += f", генерирую батч {batch_num + 1}"
    print(label)

# Merge full
full = [r for b in all_batches for r in b]
with open("full_dataset.json", "w") as f:
    json.dump(full, f, ensure_ascii=False, indent=2)

print(f"Готово, все {len(full)} записей сгенерированы")

# Stats
normals = sum(1 for r in full if r["label"] == "normal")
anomalies = sum(1 for r in full if r["label"] == "anomaly")
print(f"Нормальных: {normals}, Аномальных: {anomalies}")

for i, b in enumerate(all_batches):
    n = sum(1 for r in b if r["label"] == "normal")
    a = sum(1 for r in b if r["label"] == "anomaly")
    tricky = sum(1 for r in b if "timeout" in r["message"].lower() or "соединен" in r["message"].lower() or "retry" in r["message"].lower() or "deadlock" in r["message"].lower() or "oom" in r["message"].lower() or "cascade" in r["message"].lower() or "rate limit" in r["message"].lower())
    print(f"  Батч {i+1}: {len(b)} записей ({n} normal / {a} anomaly, ~{tricky} tricky)")

at_counts = {}
for r in full:
    if r["label"] == "anomaly":
        at_counts[r["anomaly_type"]] = at_counts.get(r["anomaly_type"], 0) + 1
print("Распределение по типам аномалий:")
for at, cnt in sorted(at_counts.items()):
    uniq = len(set(r["message"] for r in full if r["label"] == "anomaly" and r["anomaly_type"] == at))
    print(f"  {at}: {cnt} entries, {uniq} unique messages")

# Check message repetition
from collections import Counter
msg_counts = Counter(r["message"] for r in full)
repeated = {m: c for m, c in msg_counts.items() if c > 3}
if repeated:
    print(f"⚠️ Сообщения с повтором >3: {len(repeated)}")
    for m, c in list(repeated.items())[:5]:
        print(f"    [{c}x] {m[:80]}...")
else:
    print("✅ Ни одно сообщение не повторено более 3 раз")
