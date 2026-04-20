# Tests (Manual)

These scripts are manual smoke tests to verify:
1. DB update works.
2. Laravel API call works.
3. Socket + Laravel invoice flow works.

Prereqs:
1. Socket server running.
2. Laravel server running and reachable.
3. MySQL reachable if you run DB test.

Environment variables:
- `LARAVEL_BASE_URL` (default: `https://osbackend.gocab.net`)
- `SOCKET_URL` (default: `http://localhost:3000`)
- `SOCKET_HTTP_URL` (default: same as `SOCKET_URL`)

---

## 1) DB update test
Updates a ride status in `user_ride_booking` and then reads it back.

```bash
node tests/01-db-update.js --ride 12345 --status 5
```

---

## 2) Laravel invoice API test
Calls `POST /api/driver/transport-ride-invoice`.

```bash
node tests/02-api-invoice.js \
  --driver 77 \
  --service 15 \
  --token "DRIVER_ACCESS_TOKEN" \
  --ride 12345
```

---

## 3) Socket -> Laravel -> Socket invoice flow
1) Connects driver.
2) Triggers `/events/internal/ride-status-updated`.
3) Waits for `ride:invoice`.

```bash
node tests/03-socket-invoice-flow.js \
  --driver 77 \
  --service 15 \
  --token "DRIVER_ACCESS_TOKEN" \
  --ride 12345 \
  --status 7
```

---

## 4) Full E2E scenario (Driver + User + API + Events)
This script connects **driver + user sockets**, dispatches a ride, submits a bid, accepts it,
then calls Laravel `update-ride-status` to trigger `ride:statusUpdated` + `ride:ended`.

It expects valid IDs/tokens from your DB.

```bash
node tests/04-auto-e2e.js \
  --socket "http://localhost:3000" \
  --socketHttp "http://localhost:3000" \
  --laravel "https://osbackend.gocab.net" \
  --driver 7 \
  --driverService 7 \
  --driverToken "DRIVER_ACCESS_TOKEN" \
  --serviceType 2 \
  --user 2 \
  --userName "Test User" \
  --userToken "USER_ACCESS_TOKEN" \
  --ride 9 \
  --serviceCategory 5 \
  --price 500 \
  --pickupLat 33.49356 \
  --pickupLong 36.24070 \
  --destLat 33.48312 \
  --destLong 36.24139
```

---

## 5) Full flow (socket-server/tests/full-flow-e2e.js)
Runs via the helper runner (starts socket server + test):

```bash
node socket-server/tests/run-full-flow.js
```

Useful env flags:
- `SCENARIO` = `happy` (default), `timeout`, `cancel`, `multi-driver`
- `RIDE_TIMEOUT_MS` and `TIMEOUT_CHECK_MS` for timeout tests
- `DRIVER2_ID`, `DRIVER2_SERVICE_ID`, `DRIVER2_ACCESS_TOKEN` for multi-driver

Example (timeout scenario):
```bash
SCENARIO=timeout RIDE_TIMEOUT_MS=5000 TIMEOUT_CHECK_MS=9000 TEST_DURATION_MS=20000 \
node socket-server/tests/run-full-flow.js
```

---

## 6) Live passed-destination verification (real ride)
This script is built for **active real rides** to verify the flow:
`ride:passedDestination` -> decision ack -> `ride:passedDestinationAccepted` -> `ride:extraDistanceAccepted`.

By default it is **observe-only** (does not send accept/status updates).

Run:

```bash
DRIVER_ID=77 \
DRIVER_SERVICE_ID=15 \
DRIVER_ACCESS_TOKEN="DRIVER_ACCESS_TOKEN" \
RIDE_ID=12345 \
SOCKET_URL="https://socket.gocab.net" \
node tests/05-passed-destination-live.js
```

Optional flags:
- `AUTO_ACCEPT=1`: auto-send `driver:passedDestinationDecision` after `ride:passedDestination`.
- `AUTO_STATUS_7=1`: auto-send `driver:updateRideStatus` with status `7` after accepted ack.
  - requires `SERVICE_CATEGORY_ID`.
- `FORCE_DECISION=1`: send `driver:passedDestinationDecision` directly after connect (without waiting for `ride:passedDestination`).
- `CURRENT_LAT`, `CURRENT_LONG`: force coordinates sent with auto events.
- `DECISION_LAT`, `DECISION_LONG`: coordinates used specifically for the decision event.
- `STATUS7_LAT`, `STATUS7_LONG`: coordinates used specifically for status `7` update.
- `TIMEOUT_MS` (default: `180000`)
- `STRICT=1|0` (default: `1`)
  - `1`: exits non-zero unless all expected events arrive with positive `extra_distance_km`
  - `0`: summary-only mode

Example (active mode):

```bash
DRIVER_ID=77 \
DRIVER_SERVICE_ID=15 \
DRIVER_ACCESS_TOKEN="DRIVER_ACCESS_TOKEN" \
RIDE_ID=12345 \
SERVICE_CATEGORY_ID=5 \
SOCKET_URL="https://socket.gocab.net" \
AUTO_ACCEPT=1 \
AUTO_STATUS_7=1 \
node tests/05-passed-destination-live.js
```
