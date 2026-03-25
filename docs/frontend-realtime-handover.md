# Realtime Frontend Handover (Socket.IO)

This document is for the Flutter/Frontend team. It describes the **current** realtime contracts as implemented.  
No event names or payload shapes are changed.

---

## 1) Rooms (IMPORTANT)

Only these rooms are used:
- `ride:{rideId}` (main room for all ride events)
- `driver:{driverId}` (driver-specific room)

**User rooms (`user:{userId}`) are NOT used anymore.**  
All ride-related events for the user come through `ride:{rideId}` only.

---

## 2) Socket URL
- Socket Server: `http://<socket-host>:3000`

---

## 3) Driver → Socket Events

1. `driver-online`
   - Driver goes online, joins `driver:{driverId}`.

2. `update-location`
   - Driver location updates.

3. `driver:getRidesList`
   - Driver inbox list.

4. `driver:submitBid`
   - Driver submits a bid for ride.

5. `driver:acceptOffer`
   - Driver accepts the offer.

6. `driver:updateRideStatus`
   - Driver updates ride status (this calls Laravel API).

---

## 4) Socket → Driver Events

- `driver:ready`
- `ride:bidRequest`
- `driver:rides:list`
- `driver:rides:list:update`
- `ride:userAccepted`
- `ride:driverAccepted`
- `ride:statusPreUpdate`
- `ride:statusUpdated`
- `ride:ended`
- `ride:invoice`
- `ride:arrived:ack`
- `driver:moved`
- `ride:locationUpdate`

---

## 5) User → Socket Events

1. `user:loginInfo` (optional)
2. `user:findNearbyDrivers` (include `route` + `eta_min` when available)
3. `user:getNearbyVehicleTypes` (include `route` + `eta_min` when available)
4. `user:updateNearbyCenter`
5. `user:setNearbyServiceType`
6. `user:stopNearbyDrivers`
7. `user:joinRideRoom` **(REQUIRED once ride_id exists)**
8. `user:respondToDriver`
9. `user:acceptOffer`

---

## 6) Socket → User Events

All are emitted to **`ride:{rideId}`** (no user room):

- `user:nearbyDrivers`
- `user:nearbyDrivers:update`
- `user:nearbyVehicleTypes`
- `user:nearbyVehicleTypes:update`
- `ride:joined`
- `ride:newBid`
- `ride:userAccepted`
- `ride:acceptedByDriver`
- `ride:statusUpdated`
- `ride:locationUpdate`
- `ride:arrived`
- `ride:completed`
- `ride:cancelled`
- `ride:closed`
- `ride:ended`

---

## 7) Required Frontend Flow (User)

### A) Nearby drivers
1. `user:loginInfo` (optional, just for user details)
2. `user:findNearbyDrivers` with `lat`, `long`, `service_type_id` (optional)
3. Listen to:
   - `user:nearbyDrivers`
   - `user:nearbyDrivers:update`
   - `user:nearbyVehicleTypes`
   - `user:nearbyVehicleTypes:update`

### B) Join ride room (REQUIRED)
Once you get a valid `ride_id`, you MUST call:
```
user:joinRideRoom { user_id, ride_id }
```
Then listen only on `ride:{rideId}` for all ride events.

If you want `route`/`eta_min` to reach the driver, make sure the user already sent them
in `user:findNearbyDrivers` or `user:getNearbyVehicleTypes` **before** `user:joinRideRoom`.

### C) Negotiation
1. Receive `ride:newBid` from ride room.
2. Reply with `user:respondToDriver`.

### D) Accept
1. User accepts with `user:acceptOffer`.
2. Listen for:
   - `ride:userAccepted`
   - `ride:acceptedByDriver`

### E) Status & Tracking
Listen on ride room:
- `ride:statusUpdated`
- `ride:locationUpdate`
- `ride:arrived`
- `ride:ended`
- `ride:completed`
- `ride:cancelled`
- `ride:closed`

---

## 8) Notes

- **Laravel is the source of truth** for ride status.  
- Socket server just routes realtime events.
- Status events may be emitted quickly; dedupe is handled server-side.
- If you do **not** join `ride:{rideId}`, you will **miss** all ride events.
- `ride:statusPreUpdate` is emitted to the driver room **before** calling Laravel.
  It can include `route` and `eta_min` when available.
