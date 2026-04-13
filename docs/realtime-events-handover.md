# Realtime System Handover (End-to-End)

This document explains the current realtime flow between:
1. Laravel API (Backend + DB)
2. Node.js Socket Server (Socket.IO)
3. Driver App (Frontend)
4. User App (Frontend)

This is documentation only. Logic, event names, and payloads remain exactly as implemented.

## Core Concepts
1. Rooms used for targeting:
   - `driver:{driverId}` for driver-specific events
   - `user:{userId}` for user-specific events
   - `ride:{rideId}` for ride-specific events
2. Driver proximity uses memory only. `driverLocationService` stores driver location and metadata in memory and is used for nearby search. No DB proximity query is performed.
3. Laravel is the source of truth for persistence. The socket server focuses on realtime routing; tracking endpoints also update ride status via shared DB queries (`updateRideStatus`) as currently implemented.
4. Laravel triggers internal HTTP events to the socket server (for example `/events/internal/ride-bid-dispatch`, `/events/internal/ride-status-updated`).
5. The socket server acts as a realtime router and dispatcher, using rooms and in-memory state such as `driverRideInbox`, `rideCandidates`, `rideDetailsMap`, and `userActiveRide`.

## A) Driver Goes Online + Location Updates
1. Driver app connects and emits `driver-online`.
2. Server behavior:
   - Validates `driver_id`, `lat`, `long` and binds `socket.driverId`.
   - Joins room `driver:{driverId}`.
   - Stores location and metadata in `driverLocationService` (memory).
   - Calls Laravel `POST /api/driver/update-current-status` if `access_token` is present and updates memory with service type and vehicle details.
   - Emits `driver:ready` back to the driver.
3. Driver emits `update-location`:
   - Updates memory via `driverLocationService.updateMemory`.
   - Emits `driver:moved` to `driver:{driverId}`.
   - If the driver has an active ride, emits `ride:locationUpdate` to `ride:{rideId}`.
4. Laravel can also push driver location via `POST /events/internal/driver-location`, which updates memory and emits `driver:moved` and `ride:locationUpdate` (if an active ride exists).
5. Laravel can push driver status via `POST /events/internal/driver-status-updated`. The socket server:
   - Broadcasts `driver:status-updated`.
   - Joins or leaves the `drivers:online` room.
   - Removes offline drivers from memory so they stop appearing in nearby queries.

## B) User Starts Nearby Drivers Streaming
1. Optional registration:
   - User emits `user:loginInfo` (or `user:initialData`).
   - Server stores user details in memory and joins `user:{userId}`.
2. User emits `user:findNearbyDrivers` with `lat`, `long`, and optional `service_type_id`.
3. Server behavior:
   - Resets radius to 500 meters.
   - Sets `nearbyCenter` to the provided coordinates.
   - Applies `service_type_id` filter if provided.
   - Finds nearby drivers from memory (only online, max age 2 minutes).
   - If no drivers found, expands radius by x2 up to 5000 meters and retries.
   - Emits `user:nearbyDrivers` and `user:nearbyVehicleTypes` immediately.
   - Starts a 3-second interval and emits `user:nearbyDrivers:update` and `user:nearbyVehicleTypes:update`.
4. Additional nearby controls:
   - `user:getNearbyVehicleTypes` returns only types (no driver streaming).
   - `user:updateNearbyCenter` updates the center and triggers an immediate update.
   - `user:setNearbyServiceType` changes the filter, resets radius to 500, and emits updates.
   - `user:stopNearbyDrivers` stops streaming and clears nearby state.

## C) Laravel Dispatches New Ride Bidding
1. Laravel sends `POST /events/internal/ride-bid-dispatch` to the socket server.
2. `dispatchToNearbyDrivers()`:
   - Reads `ride_id`, pickup coordinates, `radius`, `service_type_id`, and bid values.
   - Clamps radius between 200m and max (default 7000m).
   - Filters nearby drivers from memory (online only, max age 2 minutes, optional service_type filter).
   - Limits candidates (default max 30).
   - Stores candidates and a ride snapshot for redispatch.
   - Enforces one active ride per user.
3. For each candidate driver:
   - Emits `ride:bidRequest` to `driver:{driverId}`.
   - Adds the ride to the driver's inbox and emits `driver:rides:list:update`.
4. Starts a ride timeout (90 seconds). If it expires, the ride is removed from inboxes and candidates.
5. User app should join the ride room using `user:joinRideRoom`, which responds with `ride:joined` and ensures the user receives ride-room broadcasts.
6. Drivers can fetch their inbox list at any time with `driver:getRidesList`, which returns `driver:rides:list`.

## D) Driver Submits Bid
1. Driver emits `driver:submitBid` with `ride_id`, `offered_price`, and `currency`.
2. Server validates the driver using `socket.driverId` and verifies the driver is a candidate.
3. Server calls Laravel `POST /api/driver/bid-offer` (requires `driver_service_id` and `access_token`).
4. Server emits `ride:newBid` to `ride:{rideId}` and also to `user:{userId}` if known.
5. The ride is removed from the driver's inbox and `driver:rides:list:update` is emitted.
6. The driver cannot submit another bid for the same ride until the user responds.

## E) User Counter Offer
1. User emits `user:respondToDriver` with `ride_id` and a new `price`.
2. Server behavior:
   - Cancels the ride timeout.
   - Updates inbox price for all candidate drivers and emits `driver:rides:list:update`.
   - Emits `ride:userResponse` to each candidate driver's room.
   - Marks the driver as allowed to bid again.
   - Redispatches the ride with the updated price.
   - Emits `ride:priceUpdated` to `ride:{rideId}`.

## F) Offer Acceptance
Option 1: User accepts (`user:acceptOffer`)
1. Server calls Laravel `POST /api/customer/transport/accept-bid` if `user_id` and `access_token` are available.
2. `finalizeAcceptedRide()`:
   - Sets active ride mapping (driver <-> ride).
   - Emits `ride:userAccepted` to `driver:{driverId}`, `ride:{rideId}`, and `user:{userId}` if known.
   - Closes bidding and clears other drivers' inboxes.
   - Emits an initial `ride:locationUpdate` if a driver location exists in memory.

Option 2: Driver accepts (`driver:acceptOffer`)
1. Server trusts `socket.driverId` (anti-spoofing) and removes the ride from the driver's inbox.
2. Emits `ride:driverAccepted` to `driver:{driverId}` and `ride:acceptedByDriver` to `ride:{rideId}` and `user:{userId}` if known.
3. If `user_id` and `access_token` are available, server calls Laravel `POST /api/customer/transport/accept-bid`.
4. Calls `finalizeAcceptedRide()` (same as user acceptance flow).

Additional acceptance from Laravel:
1. `POST /events/internal/ride-user-accepted` emits `ride:userAccepted`, sets active ride mapping, and closes bidding.

## G) Ride Status Updates From Laravel
1. Laravel sends `POST /events/internal/ride-status-updated`.
2. Server emits `ride:statusUpdated` to `ride:{rideId}`, `user:{userId}`, and `driver:{driverId}`.
3. If status is 7 or 9, server calls `POST /api/driver/transport-ride-invoice` once per ride and emits `ride:invoice` to the driver.
4. If status is final (4, 6, 7, 8, 9):
   - Clears active ride mapping.
   - Closes bidding.
   - Emits `ride:ended` to ride, user, and driver rooms.

## H) Live Tracking
1. Driver `update-location` emits `ride:locationUpdate` when an active ride exists.
2. Laravel can call:
   - `POST /ride/start-tracking` which starts tracking, updates ride status to 5, and emits `ride:locationUpdate`.
   - `POST /ride/update-location` which updates location, keeps status 5, and emits `ride:locationUpdate`.
   - `POST /ride/arrived` which emits `ride:arrived` and `ride:arrived:ack` (to the driver if `driver_id` is provided).
   - `POST /ride/stop-tracking` which stops tracking, updates status to 6, and emits `ride:completed`.
3. User app listens to `ride:locationUpdate`, `ride:arrived`, `ride:statusUpdated`, `ride:ended`, `ride:completed`, `ride:cancelled`, and `ride:closed`.

## I) Ride Cancel / Close
1. User or system can emit `ride:cancel`.
2. Server marks the ride cancelled, removes it from inboxes and candidates, clears active mapping, emits `ride:cancelled`, and calls Laravel `POST /api/customer/transport/cancel-ride`.
3. `ride:close` clears the ride from memory and emits `ride:closed`.

## Reliability Safeguards
1. Driver identity is enforced from `socket.driverId` for driver actions (anti-spoofing).
2. Ride timeouts remove rides from inboxes and candidate lists (90s).
3. Driver inbox entries are pruned by TTL (default 2 minutes) on a periodic cleanup.
4. `cancelledRides` blocks redispatch for 10 minutes after `ride:cancel`.
5. Stale drivers are filtered by max location age (default 2 minutes) in nearby and dispatch.
6. One active ride per user is enforced via `userActiveRide`.
7. Invoice deduplication uses `sentInvoiceForRide` (10-minute TTL).
8. Offline drivers are removed from memory on disconnect or `driver:status-updated`.

---

# Appendix: Event Reference (Exact Payloads)

This section lists the current event names and payload shapes as implemented.

**Socket Server**
1. Socket URL: `http://<socket-host>:3000`
2. HTTP Internal URL: `http://<socket-host>:3000`

---

**Frontend -> Socket (Driver)**
1. `driver-online`
```json
{
  "driver_id": 77,
  "lat": 33.5140,
  "long": 36.2770,
  "access_token": "DRIVER_ACCESS_TOKEN",
  "driver_service_id": 15
}
```
2. `update-location`
```json
{
  "lat": 33.5142,
  "long": 36.2774
}
```
3. `driver:getRidesList`
```json
{
  "driver_id": 77
}
```
4. `driver:submitBid`
```json
{
  "ride_id": 12345,
  "offered_price": 25000,
  "currency": 1
}
```
5. `driver:acceptOffer`
```json
{
  "ride_id": 12345,
  "driver_id": 77,
  "offered_price": 25000
}
```

---

**Socket -> Driver**
1. `driver:ready`
```json
{
  "driver_id": 77
}
```
2. `ride:bidRequest`
```json
{
  "ride_id": 12345,
  "pickup_lat": 33.5142,
  "pickup_long": 36.2774,
  "pickup_address": "Pickup Address",
  "destination_lat": 33.5201,
  "destination_long": 36.2812,
  "destination_address": "Drop Address",
  "radius": 5000,
  "user_bid_price": 25000,
  "min_fare_amount": 20000,
  "user_details": {
    "user_id": 9,
    "user_name": "User Name",
    "user_gender": 1,
    "user_image": "..."
  }
}
```
3. `driver:rides:list`
```json
{
  "driver_id": 77,
  "rides": [],
  "total": 0,
  "at": 1739050000000
}
```
4. `driver:rides:list:update`
Same payload as `driver:rides:list`.
5. `ride:userAccepted`
```json
{
  "ride_id": 12345,
  "driver_id": 77,
  "offered_price": 25000,
  "message": "User accepted the offer",
  "at": 1739050000000
}
```
6. `ride:driverAccepted`
```json
{
  "ride_id": 12345,
  "driver_id": 77,
  "offered_price": 25000,
  "message": "Offer accepted by driver",
  "ride_details": { "pickup_lat": 33.5142 },
  "at": 1739050000000
}
```
7. `ride:statusUpdated`
```json
{
  "ride_id": 12345,
  "ride_status": 5
}
```
8. `ride:ended`
```json
{
  "ride_id": 12345,
  "ride_status": 9,
  "ended": true
}
```
9. `ride:invoice` (sent on status 7 or 9)
```json
{
  "ride_id": 12345,
  "ride_status": 7,
  "invoice": { "status": 1, "message": "..." },
  "at": 1739050000000
}
```
10. `ride:arrived:ack`
```json
{
  "ride_id": 12345,
  "driver_id": 77,
  "lat": 33.5142,
  "long": 36.2774,
  "arrived_at": 1739050000000
}
```
11. `driver:moved`
```json
{
  "driver_id": 77,
  "lat": 33.5142,
  "long": 36.2774,
  "timestamp": 1739050000000
}
```

---

**Frontend -> Socket (User)**
1. `user:loginInfo`
```json
{
  "user_id": 9,
  "user_name": "User Name",
  "contact_number": "937482058",
  "select_country_code": "+963",
  "profile_image": null
}
```
2. `user:findNearbyDrivers`
```json
{
  "user_id": 9,
  "lat": 33.5138,
  "long": 36.2765,
  "service_type_id": 2
}
```
3. `user:getNearbyVehicleTypes`
```json
{
  "lat": 33.5138,
  "long": 36.2765
}
```
4. `user:updateNearbyCenter`
```json
{
  "lat": 33.5138,
  "long": 36.2765
}
```
5. `user:setNearbyServiceType`
```json
{
  "service_type_id": 2
}
```
6. `user:stopNearbyDrivers`
No payload.
7. `user:joinRideRoom`
```json
{
  "user_id": 9,
  "ride_id": 12345
}
```
8. `user:respondToDriver`
```json
{
  "ride_id": 12345,
  "driver_id": 77,
  "type": "counter",
  "price": 25000,
  "message": "..."
}
```
9. `user:acceptOffer`
```json
{
  "ride_id": 12345,
  "driver_id": 77,
  "offered_price": 25000,
  "user_id": 9,
  "access_token": "USER_ACCESS_TOKEN"
}
```

---

**Socket -> User**
1. `user:nearbyDrivers`
2. `user:nearbyDrivers:update`
3. `user:nearbyVehicleTypes`
4. `user:nearbyVehicleTypes:update`
5. `ride:joined`
6. `ride:newBid`
7. `ride:userAccepted`
8. `ride:acceptedByDriver`
9. `ride:statusUpdated`
10. `ride:locationUpdate`
11. `ride:arrived`
12. `ride:completed`
13. `ride:cancelled`
14. `ride:closed`
15. `ride:ended`

---

**Backend (Laravel) -> Socket HTTP**
1. `POST /events/internal/ride-bid-dispatch`
```json
{
  "ride_id": 12345,
  "service_category_id": 5,
  "service_type_id": 2,
  "pickup_lat": 33.5142,
  "pickup_long": 36.2774,
  "pickup_address": "...",
  "destination_lat": 33.5201,
  "destination_long": 36.2812,
  "destination_address": "...",
  "radius": 5000,
  "user_bid_price": 25000,
  "min_fare_amount": 20000
}
```
2. `POST /events/internal/ride-user-accepted`
```json
{
  "ride_id": 12345,
  "driver_id": 77,
  "offered_price": 25000
}
```
3. `POST /events/internal/ride-status-updated`
```json
{
  "ride_id": 12345,
  "driver_id": 77,
  "ride_status": 7,
  "lat": 33.5142,
  "long": 36.2774,
  "payload": {}
}
```
4. `POST /events/internal/ride-dispatch-retry`
```json
{
  "ride_id": 12345,
  "pickup_lat": 33.5142,
  "pickup_long": 36.2774,
  "radius": 5000,
  "user_bid_price": 25000,
  "min_fare_amount": 20000
}
```
5. `POST /events/internal/driver-status-updated`
```json
{
  "driver_id": 77,
  "old_status": 0,
  "new_status": 1
}
```
6. `POST /events/internal/driver-location`
```json
{
  "driver_id": 77,
  "lat": 33.5142,
  "lng": 36.2774
}
```
7. `POST /ride/start-tracking`
```json
{
  "ride_id": 12345,
  "pickup_lat": 33.5142,
  "pickup_long": 36.2774,
  "destination_lat": 33.5201,
  "destination_long": 36.2812
}
```
8. `POST /ride/update-location`
```json
{
  "ride_id": 12345,
  "lat": 33.5142,
  "long": 36.2774
}
```
9. `POST /ride/stop-tracking`
```json
{
  "ride_id": 12345
}
```
10. `POST /ride/arrived`
```json
{
  "ride_id": 12345,
  "driver_id": 77,
  "lat": 33.5142,
  "long": 36.2774,
  "arrived_at": 1739050000000
}
```

---

**Socket -> Laravel API Calls**
1. `POST /api/customer/transport/accept-bid` (from `user:acceptOffer`)
2. `POST /api/driver/bid-offer` (from `driver:submitBid`)
3. `POST /api/driver/transport-ride-invoice` (on `ride_status` 7 or 9)
4. `POST /api/customer/transport/cancel-ride` (when user cancels in socket)
