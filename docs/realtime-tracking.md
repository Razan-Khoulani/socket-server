# Real-time Tracking & Events (Driver / Customer)

هذا الملف مخصص لمطوّرَي الفرونت (السائق + الزبون) لتجربة وتتبع الرحلة عبر السوكت.

**ملخص سريع**
1. الزبون ينضم لغرفة الرحلة عبر `user:joinRideRoom`.
2. السائق يرسل لوكيشن عبر `update-location` بشكل دوري.
3. السيرفر يبث للزبون `ride:locationUpdate` في نفس غرفة الرحلة.
4. كل تحديث حالة من Laravel يخرج Event واحد: `ride:statusUpdated`.
5. عند إنهاء الرحلة يخرج Event جديد: `ride:ended`.

**أحداث الزبون (Customer -> Socket)**
1. `user:loginInfo` (اختياري لتخزين بيانات المستخدم في الذاكرة)
2. `user:findNearbyDrivers`
3. `user:updateNearbyCenter`
4. `user:setNearbyServiceType`
5. `user:stopNearbyDrivers`
6. `user:joinRideRoom`
7. `user:respondToDriver` (التفاوض على السعر في نظام البِدّينغ)

**Payload أمثلة (Customer -> Socket)**
```json
// user:loginInfo
{
  "user_id": 9,
  "user_name": "فداء ديراني",
  "contact_number": "937482058",
  "select_country_code": "+963",
  "profile_image": null
}
```
```json
// user:findNearbyDrivers
{
  "user_id": 9,
  "lat": 33.5138,
  "long": 36.2765,
  "service_type_id": 2
}
```
```json
// user:joinRideRoom
{
  "user_id": 9,
  "ride_id": 12345
}
```

**أحداث الزبون التي يستقبلها (Socket -> Customer)**
1. `user:nearbyDrivers`
2. `user:nearbyDrivers:update`
3. `user:nearbyVehicleTypes`
4. `user:nearbyVehicleTypes:update`
5. `ride:joined`
6. `ride:newBid`
7. `ride:userAccepted`
8. `ride:statusUpdated`
9. `ride:locationUpdate`
10. `ride:arrived`
11. `ride:completed`
12. `ride:cancelled`
13. `ride:closed`
14. `ride:ended`

**أحداث السائق (Driver -> Socket)**
1. `driver-online`
2. `update-location` (إرسال لوكيشن دوري)
3. `driver:getRidesList`
4. `driver:submitBid`

**Payload أمثلة (Driver -> Socket)**
```json
// driver-online
{
  "driver_id": 77,
  "lat": 33.5140,
  "long": 36.2770,
  "access_token": "driver-access-token",
  "driver_service_id": 15
}
```
```json
// update-location
{
  "lat": 33.5142,
  "long": 36.2774
}
```
```json
// driver:submitBid
{
  "ride_id": 12345,
  "offered_price": 25000,
  "currency": 1
}
```

**أحداث السائق التي يستقبلها (Socket -> Driver)**
1. `driver:ready`
2. `ride:bidRequest`
3. `driver:rides:list`
4. `driver:rides:list:update`
5. `ride:userAccepted`
6. `ride:driverAccepted`
7. `ride:statusUpdated`
8. `ride:arrived:ack`
9. `ride:ended`
10. `ride:invoice`
11. `driver:moved`

**Realtime Status Update (Event واحد فقط)**
الفرونت يسمع `ride:statusUpdated` في غرفة الرحلة وغرفة السائق.
```json
{
  "ride_id": 12345,
  "ride_status": 5
}
```

**Realtime Tracking (اللوكيشن)**
السائق يرسل `update-location`، والسيرفر يبث `ride:locationUpdate` لكل من في غرفة الرحلة:
```json
{
  "ride_id": 12345,
  "driver_id": 77,
  "lat": 33.5142,
  "long": 36.2774,
  "at": 1739050000000
}
```

**End Event**
عند انتهاء الرحلة أو إلغائها نهائياً، السيرفر يرسل:
`ride:ended`
استخدمه لإيقاف التتبع على الخريطة وإغلاق الرحلة في الفرونت.

**Laravel -> Node (Endpoints مهمة للريـل تايم)**
1. `POST /events/internal/ride-bid-dispatch`
2. `POST /events/internal/ride-user-accepted`
3. `POST /events/internal/ride-status-updated`
4. `POST /ride/start-tracking`
5. `POST /ride/update-location`
6. `POST /ride/stop-tracking`
7. `POST /ride/arrived`
8. `POST /events/internal/driver-location`
9. `POST /events/internal/driver-status-updated`

**خطوات تجربة سريعة**
1. الزبون يتصل بالسوكيت ويرسل `user:loginInfo` ثم `user:findNearbyDrivers`.
2. السائق يتصل ويرسل `driver-online`.
3. النظام يرسل له `ride:bidRequest`.
4. السائق يرسل `driver:submitBid`.
5. الزبون يقبل في Laravel ويُستدعى `/events/internal/ride-user-accepted`.
6. الزبون يعمل `user:joinRideRoom`.
7. السائق يبدأ إرسال `update-location` كل 2-3 ثواني.
8. الزبون يستقبل `ride:locationUpdate`.
9. تحديثات الحالة تأتي عبر `ride:statusUpdated`.
10. عند النهاية ستصل `ride:ended`.

**ملاحظات مهمة**
1. لا تبث `update-location` إلا بعد قبول السائق.
2. أي تحديث حالة من Laravel يجب أن يمر عبر `/events/internal/ride-status-updated`.
3. عند وصول `ride:ended` أوقف التتبع فوراً في الفرونت.
