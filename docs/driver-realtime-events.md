# Driver Realtime Events (Socket -> Driver)

??? ????? ????? ??? ??? ??????? ???? ?????? ??????? ???????? (Socket -> Driver).

## 1) driver:ready
??? ??? ???? ????? ??????.
```json
{
  "driver_id": 77
}
```

## 2) ride:bidRequest
??? ??? ???? ??? ?????? ??????.
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

## 3) driver:rides:list
????? ??????? ?? ????? ??????.
```json
{
  "driver_id": 77,
  "rides": [
    {
      "ride_id": 12345,
      "user_id": 9,
      "user_name": "User Name",
      "pickup_lat": 33.5142,
      "pickup_long": 36.2774,
      "pickup_address": "...",
      "destination_lat": 33.5201,
      "destination_long": 36.2812,
      "destination_address": "...",
      "radius": 5000,
      "user_bid_price": 25000,
      "min_fare_amount": 20000,
      "service_type_id": 2,
      "service_category_id": 5,
      "created_at": "2026-02-11 10:20:00",
      "meta": {},
      "user_details": {
        "user_id": 9,
        "user_name": "User Name"
      }
    }
  ],
  "total": 1,
  "at": 1739050000000
}
```

## 4) driver:rides:list:update
??? payload `driver:rides:list` ??? ??? ????? ???????.

## 5) ride:userAccepted
??? ????? ?????? ???? ?????.
```json
{
  "ride_id": 12345,
  "driver_id": 77,
  "offered_price": 25000,
  "message": "User accepted the offer",
  "at": 1739050000000
}
```

## 6) ride:driverAccepted
Sent when the driver accepts an offer.
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

## 7) ride:statusUpdated

```json
{
  "ride_id": 12345,
  "ride_status": 5
}
```
??????: ???? `ride_status` ?? ??? ????? ?? ?????-???.

## 8) ride:ended
 (for this status : 4,6,7,8,9).
```json
{
  "ride_id": 12345,
  "ride_status": 9,
  "ended": true
}
```

## 9) ride:invoice
Sent to driver only when status is 7 or 9.
```json
{
  "ride_id": 12345,
  "ride_status": 7,
  "invoice": { "status": 1, "message": "..." },
  "at": 1739050000000
}
```

## 10) ride:arrived:ack
Ack ?????? ????? Laravel ???? ??? ??????.
```json
{
  "ride_id": 12345,
  "driver_id": 77,
  "lat": 33.5142,
  "long": 36.2774,
  "arrived_at": 1739050000000
}
```

## 11) driver:moved
Ack ????? ?????? ???? (??????? ???? ??????).
```json
{
  "driver_id": 77,
  "lat": 33.5142,
  "long": 36.2774,
  "timestamp": 1739050000000
}
```

---

## Note
Status tracking uses:
- `ride:statusUpdated`
- `ride:ended`
Invoice is sent on:
- `ride:invoice`
