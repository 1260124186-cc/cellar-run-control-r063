# Cellar Run Control

Cellar Run Control is a local Go HTTP service for coordinating a small
production cellar. It keeps formula approval, vessel availability, active
fermentation runs, gravity observations, conditioning, completion metrics, and
vessel cleaning in one deterministic process.

This repository is a healthy initialization baseline. It has no intentional
defects and no automated test suite. Tests are intentionally deferred to a
later engineering task; the production workflow checks in this repository
remain available for immediate build and behavior verification.

## Requirements

- Go 1.23 or later
- A writable local data directory
- No database, network service, or third-party module

## Build And Run

Build every package:

```bash
go build ./...
```

Start the service:

```bash
go run ./cmd/fermentctl serve
```

The default address is `127.0.0.1:8088`. The default snapshot directory is
`./data`.

Environment variables:

- `CELLAR_CONTROL_ADDR`: HTTP listen address.
- `CELLAR_CONTROL_DATA_DIR`: directory containing `state.json`.

Example:

```bash
CELLAR_CONTROL_ADDR=127.0.0.1:9099 \
CELLAR_CONTROL_DATA_DIR=/tmp/cellar-control \
go run ./cmd/fermentctl serve
```

## Directory Structure

```text
cmd/fermentctl                 service and workflow-check executable
internal/clock                 time boundary
internal/config                environment configuration
internal/observability         operation ids, process identity, durable journal
internal/domain                entities, validation, transitions, metrics
internal/httpapi               HTTP routes, JSON decoding, errors, correlation
internal/service               use-case orchestration
internal/storage               revisioned snapshot persistence
internal/workflowcheck         real HTTP production checks
```

## Public API

Health:

- `GET /healthz`

Formula workflow:

- `POST /v1/formulas`
- `GET /v1/formulas`
- `GET /v1/formulas/{id}`
- `PUT /v1/formulas/{id}/steps`
- `POST /v1/formulas/{id}/approve`
- `POST /v1/formulas/{id}/retire`

Vessel workflow:

- `POST /v1/vessels`
- `GET /v1/vessels`
- `GET /v1/vessels/{id}`
- `POST /v1/vessels/{id}/finish-cleaning`
- `POST /v1/vessels/{id}/retire`

Run workflow:

- `POST /v1/runs`
- `GET /v1/runs`
- `GET /v1/runs/{id}`
- `POST /v1/runs/{id}/reserve`
- `POST /v1/runs/{id}/start`
- `POST /v1/runs/{id}/observations`
- `POST /v1/runs/{id}/conditioning`
- `POST /v1/runs/{id}/complete`
- `POST /v1/runs/{id}/abort`

Writes accept an optional `X-Expected-Version` header. Errors use a stable
shape:

```json
{
  "error": {
    "code": "revision_conflict",
    "detail": "the aggregate version does not match X-Expected-Version",
    "field": "",
    "operation_id": "op_2f3a9c1e7b4d4086b1e8f6a2d0c9e531"
  }
}
```

## Request Correlation

Every request (and every terminal retry of the same action) carries a stable
operation id that ties together the HTTP response, the business error, the
durable snapshot write, and the command-line workflow checks.

- Send `X-Operation-Id: op_<16-32 lowercase hex digits>` on a request. When it
  is absent the server generates one; a malformed value is rejected with `400`
  and a replacement id is returned in the response header.
- A retry keeps the same `X-Operation-Id` and increments
  `X-Operation-Attempt` (defaults to `1`). Concurrent requests never share an
  id: the id and the per-request outcome collector live on the request context.
- The id is echoed on every response via the `X-Operation-Id` header and inside
  the JSON error body as `error.operation_id`.
- Each attempt is appended, fsynced, to `<data-directory>/operations.log` as
  one JSON line holding the method, path, status, attempt number, handling
  instance, stable error code, and the storage outcome/revision. The journal
  never contains request bodies, formula notes, or observation notes.
- Relocate an operation after a restart through the API:

  ```text
  GET /v1/operations/{id}
  ```

  or offline from the terminal:

  ```bash
  fermentctl operation op_2f3a9c1e7b4d4086b1e8f6a2d0c9e531
  ```

Each served attempt also emits one structured stderr line, for example:

```text
op=op_... attempt=1 method=POST path="/v1/formulas" status=201 duration_ms=3 error_code="" storage=ok revision=1
```

`GET /healthz` identifies the current process in addition to snapshot health:

```json
{
  "status": "ok",
  "revision": 3,
  "instance_id": "op_2f3a9c1e7b4d4086b1e8f6a2d0c9e531",
  "pid": 6616,
  "started_at": "2026-10-05T14:20:05.21Z",
  "host": "cellar-host",
  "current_operation": "op_2f3a9c1e7b4d4086b1e8f6a2d0c9e531"
}
```

`instance_id` is regenerated on every start, so health output distinguishes
processes while the durable operation journal keeps old operations locatable.

## Workflow Checks

Run each production workflow check separately:

```bash
go run ./cmd/fermentctl check formula-approval
go run ./cmd/fermentctl check vessel-run-lifecycle
go run ./cmd/fermentctl check observations-completion
go run ./cmd/fermentctl check operation-tracing
```

Each check creates a temporary snapshot directory, starts the real HTTP
handler on a loopback listener, issues JSON requests, verifies success and
failure behavior, and removes its temporary data. The operation-tracing check
additionally simulates a process restart, concurrent requests, terminal
retries, and a durable snapshot-write failure.

## Persistence And Recovery

The service stores a complete revisioned snapshot at
`<data-directory>/state.json`. A write creates a temporary file in the same
directory, commits the encoded bytes durably, closes the handle, and renames
the result over the previous snapshot. The in-memory snapshot is replaced only
after that operation succeeds.

On startup, an existing snapshot is decoded and normalized. Unknown or
conflicting map indexes are not silently invented; invalid JSON or a negative
revision stops the process before HTTP serving begins.

## Repair-Ready Boundaries

The baseline intentionally keeps several independent behavior surfaces visible
for later engineering evaluation:

- formula name normalization and duplicate detection;
- target gravity and process-step range validation;
- aggregate version conflict checks;
- atomic vessel reservation and release;
- run state transition guards;
- observation sequence and time ordering;
- gravity movement and range checks;
- metric calculation and rounding;
- durable snapshot replacement and startup normalization.

These are production mechanisms, not injected defects. A later task can add
automated tests or request a focused repair without changing the public API.

## Test Boundary

This stage does not create unit tests, integration tests, test fixtures, or an
E2E suite. The later test task owns systematic coverage of validation limits,
state transitions, concurrency, persistence failures, and numeric edge cases.

## Scope

The service does not control physical equipment, authenticate remote users,
send notifications, expose a browser interface, or replicate data across
processes. It is intended for a trusted local network or an internal adapter.
