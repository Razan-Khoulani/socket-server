# Realtime Handover (Frontend Dev)

This is a concise handover for Flutter/Frontend teams, reflecting the **current implementation** and **recent fixes**.  
No event names or payloads were changed. This document only explains how to use them correctly.

---

## 1) URLs
- Socket Server: `http://<socket-host>:3000`
- Laravel API: `http://<laravel-host>:8000`

---

## 2) Rooms (must keep)
- `driver:{driverId}` — private driver room  
- `user:{userId}` — private user room  
- `ride:{rideId}` — ride room (both user + driver can join)

These rooms are required to receive targeted events like `ride:statusUpdated`, `ride:locationUpdate`, etc.

---

## 3) End-to-end Flow (Code-Level)

### A) Driver online + location
1. Driver connects and emits **`driver-online`**  
2. Socket server:
   - joins `driver:{driverId}`
   - stores driver location & meta in memory  
   - calls Laravel `/api/driver/update-current-status`
   - emits **`driver:ready`**
3. Driver emits **`update-location`** repeatedly  
4. Socket emits:
   - **`driver:moved`** to `driver:{driverId}`
   - **`ride:locationUpdate`** to `ride:{rideId}` if active

---

### B) User starts nearby drivers stream
1. Optional: user emits **`user:loginInfo`**  
2. User emits **`user:findNearbyDrivers`**  
3. Socket emits:
   - **`user:nearbyDrivers`**
   - **`user:nearbyDrivers:update`**
   - **`user:nearbyVehicleTypes`**
   - **`user:nearbyVehicleTypes:update`**

Notes:
- Radius starts at 500m and expands (2x) until 5000m when no drivers.
- Optional filter: `service_type_id`

---

### C) Dispatch ride bidding (Laravel → Socket)
1. Laravel calls:
   - `POST /events/internal/ride-bid-dispatch`
2. Socket:
   - finds nearby drivers from memory
   - emits **`ride:bidRequest`**
   - updates inbox via **`driver:rides:list:update`**
   - starts timeout

---

### D) Driver submits bid
1. Driver emits **`driver:submitBid`**
2. Socket uses `socket.driverId` for anti-spoof
3. Socket calls Laravel:
   - `POST /api/driver/bid-offer`
4. Socket emits:
   - **`ride:newBid`** → `ride:{rideId}`
   - **`ride:newBid`** → `user:{userId}`

Bid is persisted in Laravel `bidding_history`.

---

### E) User counter offer
1. User emits **`user:respondToDriver`**
2. Socket:
   - cancels previous timeout
   - updates driver inbox price
   - emits **`driver:rides:list:update`**
   - emits **`ride:userResponse`**
   - re-dispatches ride with updated price

---

### F) Offer acceptance
**Option 1: User accepts**
1. User emits **`user:acceptOffer`**
2. Socket calls Laravel:
   - `POST /api/customer/transport/accept-bid`
3. Socket:
   - finalizes accepted ride
   - emits **`ride:userAccepted`**
   - closes bidding / clears other inboxes
   - sets active ride mapping

**Option 2: Driver accepts**
1. Driver emits **`driver:acceptOffer`**
2. Socket emits:
   - **`ride:driverAccepted`**
   - **`ride:acceptedByDriver`**
3. Socket calls Laravel accept API if possible
4. finalize accepted ride

---

### G) Ride status updates (Driver → Socket → Laravel → Socket)
Driver emits **`driver:updateRideStatus`**, socket calls Laravel:
`POST /api/driver/update-ride-status`

Laravel updates DB **then** calls:
`POST /events/internal/ride-status-updated`

Socket broadcasts:
- **`ride:statusUpdated`** → `ride:{rideId}`, `user:{userId}`, `driver:{driverId}`
- **`ride:ended`** on final statuses (4,6,7,8,9)

Important:
- `driver:updateRideStatus` must include required fields:
  - `ride_status=5`: needs `destination_*`, `estimated_time`, `total_distance`
  - `ride_status=6`: needs `route_lat_long_list`
  - `ride_status=7`: needs `total_amount`
  - `ride_status=3`: ride must already be accepted (status 1)

---

### H) Tracking
Laravel can call:
- `/ride/start-tracking`
- `/ride/update-location`
- `/ride/stop-tracking`
- `/ride/arrived`

User should listen:
`ride:locationUpdate`, `ride:arrived`, `ride:statusUpdated`, `ride:ended`

---

## 4) Changes Applied (Important for Frontend)
1. **Laravel `postCustomerAcceptBid` now sets ride status = 1**  
   This is required for driver status updates to work.

2. **Driver status event passes extra fields**  
   `driver:updateRideStatus` now supports additional fields for statuses 5/6/7.

3. **Tracking start uses fallback from pickup/destination latlong**  
   Laravel now sends valid coords even when fields are missing.

4. **Socket server uses correct LARAVEL URL**  
   `LARAVEL_BASE_URL` or `LARAVEL_URL` accepted.

5. **Test runner added**  
   `socket-server/tests/run-full-flow.js` starts server + runs flow test.

---

## 5) How To Re-test Full Flow (Dev)
Example PowerShell:
```powershell
$env:SOCKET_URL="http://192.168.43.240:3000"
$env:SOCKET_HTTP_URL="http://192.168.43.240:3000"
$env:LARAVEL_URL="http://192.168.43.240:8000"

$env:DRIVER_ID="7"
$env:DRIVER_SERVICE_ID="7"
$env:SERVICE_CATEGORY_ID="5"
$env:SERVICE_TYPE_ID="3"
$env:DRIVER_ACCESS_TOKEN="34716092026130245"

$env:USER_ID="9"
$env:USER_ACCESS_TOKEN="191122092026130292"

$env:RIDE_ID="1"

$env:PICKUP_LAT="33.4935628"
$env:PICKUP_LONG="36.2406966"
$env:DEST_LAT="33.498339615409"
$env:DEST_LONG="36.244756439568"

$env:STATUS_MODE="socket-event"
$env:STATUS_SEQUENCE="3,5,6,7,9"
$env:TOTAL_AMOUNT="6000"

node .\socket-server\tests\run-full-flow.js
```

---

## 6) Suggestions to avoid old rides reaching drivers
These are safe improvements (backward-compatible):
1. **Expire in-memory rides**  
   Add `expires_at` / TTL and skip dispatch if expired.
2. **On reconnect, send only pending + fresh rides**  
   Filter by `created_at` and `status === pending`.
3. **Clear inbox after accept/cancel**  
   Ensure `closeRideBidding()` removes the ride from all drivers.
4. **Block redispatch for cancelled rides**  
   Use `cancelledRides` set (already supported).
5. **Add `ride_version` or `dispatch_id`**  
   Prevent reprocessing stale bids.

---

## 7) Rooms Guarantee (Important)
Keep both rooms always:
- driver stays in `driver:{driverId}`
- user stays in `user:{userId}`
- both join `ride:{rideId}` after accept

This guarantees that:
`ride:statusUpdated`, `ride:locationUpdate`, `ride:ended` reach the correct devices.

