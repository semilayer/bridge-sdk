# Writing integration workflows for bridges

Each bridge that talks to a real external system should have a dedicated GitHub Actions workflow that spins up that system and runs `pnpm test:integration` against it. The MariaDB workflow (`integration-mariadb.yml`) is the canonical reference.

Integration workflows run only on pull requests that touch the relevant package, keeping CI fast — unit tests for every package run in the main `ci.yml` workflow instead.

---

## Anatomy of an integration workflow

```yaml
name: Integration – <DisplayName>

on:
  pull_request:
    branches: [main]
    paths:
      - 'packages/bridge-<name>/**'
      - '.github/workflows/integration-<name>.yml'
  workflow_dispatch:

env:
  FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: true

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

jobs:
  integration:
    name: <DisplayName> integration
    runs-on: ubuntu-latest

    services:
      <name>:
        image: <docker-image>
        env:
          # seed credentials here
        ports:
          - <host-port>:<container-port>
        options: >-
          --health-cmd="<healthcheck>"
          --health-interval=5s
          --health-timeout=5s
          --health-retries=10
          --health-start-period=30s

    steps:
      - uses: actions/checkout@v4

      - uses: pnpm/action-setup@v4
        with:
          version: 10.33.0

      - uses: actions/setup-node@v4
        with:
          node-version: '24'
          cache: 'pnpm'

      - name: Install dependencies
        run: pnpm install --frozen-lockfile

      - name: Build bridge-<name> and its deps
        run: pnpm turbo build --filter=@semilayer/bridge-<name>...

      - name: Run integration tests
        working-directory: packages/bridge-<name>
        env:
          DATABASE_URL: <protocol>://testuser:testpass@127.0.0.1:<port>/testdb
        run: pnpm test:integration
```

The `--filter=@semilayer/bridge-<name>...` flag builds the package **and all its workspace dependencies** (bridge-sdk, core) before running tests.

---

## Service-per-database reference

### Self-hosted (Docker services — no secrets required)

These spin up a real database container inside the GHA runner. No credentials or secrets needed.

#### PostgreSQL

```yaml
services:
  postgres:
    image: postgres:17
    env:
      POSTGRES_USER: testuser
      POSTGRES_PASSWORD: testpass
      POSTGRES_DB: testdb
    ports:
      - 5432:5432
    options: >-
      --health-cmd="pg_isready -U testuser"
      --health-interval=5s
      --health-timeout=5s
      --health-retries=10
```

```yaml
env:
  DATABASE_URL: postgres://testuser:testpass@127.0.0.1:5432/testdb
```

#### MySQL

```yaml
services:
  mysql:
    image: mysql:8
    env:
      MYSQL_ROOT_PASSWORD: testroot
      MYSQL_DATABASE: testdb
      MYSQL_USER: testuser
      MYSQL_PASSWORD: testpass
    ports:
      - 3306:3306
    options: >-
      --health-cmd="mysqladmin ping -h 127.0.0.1 -u testuser -ptestpass"
      --health-interval=5s
      --health-timeout=5s
      --health-retries=10
      --health-start-period=30s
```

```yaml
env:
  DATABASE_URL: mysql://testuser:testpass@127.0.0.1:3306/testdb
```

#### MariaDB

```yaml
services:
  mariadb:
    image: mariadb:11
    env:
      MYSQL_ROOT_PASSWORD: testroot
      MYSQL_DATABASE: testdb
      MYSQL_USER: testuser
      MYSQL_PASSWORD: testpass
    ports:
      - 3306:3306
    options: >-
      --health-cmd="healthcheck.sh --connect --innodb_initialized"
      --health-interval=5s
      --health-timeout=5s
      --health-retries=10
      --health-start-period=30s
```

```yaml
env:
  DATABASE_URL: mariadb://testuser:testpass@127.0.0.1:3306/testdb
```

#### MongoDB

```yaml
services:
  mongodb:
    image: mongo:7
    env:
      MONGO_INITDB_ROOT_USERNAME: testuser
      MONGO_INITDB_ROOT_PASSWORD: testpass
      MONGO_INITDB_DATABASE: testdb
    ports:
      - 27017:27017
    options: >-
      --health-cmd="mongosh --quiet --eval 'db.runCommand({ping:1})'"
      --health-interval=5s
      --health-timeout=5s
      --health-retries=10
      --health-start-period=20s
```

```yaml
env:
  DATABASE_URL: mongodb://testuser:testpass@127.0.0.1:27017/testdb?authSource=admin
```

#### Redis / Upstash (self-hosted)

> Upstash has a serverless cloud product but also supports a standard Redis wire protocol. For integration testing, run a plain Redis container.

```yaml
services:
  redis:
    image: redis:7
    ports:
      - 6379:6379
    options: >-
      --health-cmd="redis-cli ping"
      --health-interval=5s
      --health-timeout=5s
      --health-retries=10
```

```yaml
env:
  DATABASE_URL: redis://127.0.0.1:6379
```

#### Elasticsearch

```yaml
services:
  elasticsearch:
    image: elasticsearch:8.13.0
    env:
      discovery.type: single-node
      xpack.security.enabled: false
      ES_JAVA_OPTS: -Xms512m -Xmx512m
    ports:
      - 9200:9200
    options: >-
      --health-cmd="curl -sf http://localhost:9200/_cluster/health"
      --health-interval=10s
      --health-timeout=10s
      --health-retries=10
      --health-start-period=60s
```

```yaml
env:
  DATABASE_URL: http://127.0.0.1:9200
```

#### ClickHouse

```yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:24
    ports:
      - 8123:8123
      - 9000:9000
    options: >-
      --health-cmd="wget -qO- http://localhost:8123/ping"
      --health-interval=5s
      --health-timeout=5s
      --health-retries=10
      --health-start-period=20s
```

```yaml
env:
  DATABASE_URL: http://default:@127.0.0.1:8123/default
```

#### CockroachDB

```yaml
services:
  cockroachdb:
    image: cockroachdb/cockroach:latest-v24.1
    command: start-single-node --insecure
    ports:
      - 26257:26257
    options: >-
      --health-cmd="curl -sf http://localhost:8080/health"
      --health-interval=5s
      --health-timeout=5s
      --health-retries=10
      --health-start-period=30s
```

```yaml
env:
  DATABASE_URL: postgres://root@127.0.0.1:26257/defaultdb?sslmode=disable
```

#### Cassandra

```yaml
services:
  cassandra:
    image: cassandra:5
    ports:
      - 9042:9042
    options: >-
      --health-cmd="cqlsh -e 'describe cluster'"
      --health-interval=10s
      --health-timeout=10s
      --health-retries=15
      --health-start-period=90s
```

```yaml
env:
  DATABASE_URL: cassandra://127.0.0.1:9042/testkeyspace
```

Cassandra requires you to create the keyspace before running tests. Add a step after `pnpm install`:

```yaml
- name: Create Cassandra keyspace
  run: |
    docker exec ${{ job.services.cassandra.id }} \
      cqlsh -e "CREATE KEYSPACE IF NOT EXISTS testkeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
```

#### SQL Server (MSSQL)

```yaml
services:
  mssql:
    image: mcr.microsoft.com/mssql/server:2022-latest
    env:
      ACCEPT_EULA: Y
      SA_PASSWORD: TestPass123!
      MSSQL_PID: Developer
    ports:
      - 1433:1433
    options: >-
      --health-cmd="/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P TestPass123! -No -Q 'SELECT 1'"
      --health-interval=10s
      --health-timeout=10s
      --health-retries=10
      --health-start-period=30s
```

```yaml
env:
  DATABASE_URL: mssql://sa:TestPass123!@127.0.0.1:1433/master
```

Note: SA passwords must meet SQL Server complexity requirements (uppercase, lowercase, digit, special char, ≥8 chars).

#### DuckDB / SQLite

No service needed — both are embedded/file-based. The integration test connects to `:memory:` or a temp file path.

```yaml
- name: Run integration tests
  working-directory: packages/bridge-<name>
  run: pnpm test:integration
```

---

### Cloud-only (requires repository secrets)

These databases have no self-hostable Docker image or the integration value comes specifically from the managed service. Store credentials as [encrypted repository secrets](https://docs.github.com/en/actions/security-guides/encrypted-secrets) and gate the workflow with `if: env.DATABASE_URL != ''` so forks without secrets simply skip.

Pattern:

```yaml
- name: Run integration tests
  if: env.DATABASE_URL != ''
  working-directory: packages/bridge-<name>
  env:
    DATABASE_URL: ${{ secrets.<NAME>_DATABASE_URL }}
  run: pnpm test:integration
```

| Bridge | Secret name | Notes |
|---|---|---|
| `bridge-neon` | `NEON_DATABASE_URL` | Serverless Postgres — `postgres://...@<host>.neon.tech/neondb?sslmode=require` |
| `bridge-turso` | `TURSO_DATABASE_URL`, `TURSO_AUTH_TOKEN` | `libsql://...turso.io` + bearer token |
| `bridge-planetscale` | `PLANETSCALE_DATABASE_URL` | `mysql://...@aws.connect.psdb.cloud/<db>?ssl={"rejectUnauthorized":true}` |
| `bridge-supabase` | `SUPABASE_URL`, `SUPABASE_KEY` | Project URL + `service_role` key |
| `bridge-d1` | `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID`, `D1_DATABASE_ID` | Uses Wrangler to proxy queries |
| `bridge-upstash` (cloud) | `UPSTASH_REDIS_REST_URL`, `UPSTASH_REDIS_REST_TOKEN` | REST API, not raw Redis |
| `bridge-dynamodb` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | Or use OIDC federation — see below |
| `bridge-firestore` | `GCP_SA_KEY` (JSON) | Service account with Firestore read/write |
| `bridge-bigquery` | `GCP_SA_KEY` (JSON), `BIGQUERY_PROJECT` | Service account with BigQuery job runner role |
| `bridge-snowflake` | `SNOWFLAKE_DATABASE_URL` | `snowflake://user:pass@account/db/schema?warehouse=WH` |
| `bridge-oracle` | `ORACLE_DATABASE_URL` | Oracle Cloud Autonomous DB or on-prem with wallet |

#### DynamoDB with LocalStack (no AWS account required)

As an alternative to real AWS credentials, use [LocalStack](https://localstack.cloud) to emulate DynamoDB locally inside the runner:

```yaml
services:
  localstack:
    image: localstack/localstack:3
    ports:
      - 4566:4566
    env:
      SERVICES: dynamodb
    options: >-
      --health-cmd="curl -sf http://localhost:4566/_localstack/health"
      --health-interval=5s
      --health-timeout=5s
      --health-retries=10
      --health-start-period=30s
```

```yaml
env:
  AWS_ACCESS_KEY_ID: test
  AWS_SECRET_ACCESS_KEY: test
  AWS_REGION: us-east-1
  DYNAMODB_ENDPOINT: http://127.0.0.1:4566
```

The bridge test file should check for `DYNAMODB_ENDPOINT` and pass it as the `endpoint` option to the AWS SDK client.

#### Firestore / BigQuery with the Firebase emulator

For Firestore, the Firebase emulator is often preferable to a real GCP project:

```yaml
- name: Start Firebase emulator
  run: npx firebase-tools emulators:start --only firestore &

- name: Wait for emulator
  run: npx wait-on http://127.0.0.1:8080

- name: Run integration tests
  working-directory: packages/bridge-firestore
  env:
    FIRESTORE_EMULATOR_HOST: 127.0.0.1:8080
    GCLOUD_PROJECT: test-project
  run: pnpm test:integration
```

---

## Naming conventions

- Workflow file: `.github/workflows/integration-<name>.yml`
- Workflow name: `Integration – <DisplayName>` (em-dash, not hyphen)
- Job name: `<DisplayName> integration`
- Service container name: matches the database name (e.g., `mariadb`, `postgres`, `redis`)

## Path filters

Always include both the package path and the workflow file itself so editing the workflow triggers a re-run:

```yaml
paths:
  - 'packages/bridge-<name>/**'
  - '.github/workflows/integration-<name>.yml'
```

## Health checks

Every service container must have `--health-*` options. The job will fail immediately (rather than with a confusing connection error) if the container never becomes healthy. Use `--health-start-period` generously for databases with slow startup (Cassandra, Elasticsearch, Oracle).

## Existing workflows

| Workflow | Database | Status |
|---|---|---|
| [`integration-mariadb.yml`](../.github/workflows/integration-mariadb.yml) | MariaDB 11 | Live |
