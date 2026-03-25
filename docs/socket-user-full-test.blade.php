<!doctype html>
<html lang="ar" dir="rtl">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Socket User Full Test</title>
  <style>
    body{font-family:Arial;margin:16px;background:#f7f7f7}
    .card{background:#fff;border:1px solid #e6e6e6;border-radius:12px;padding:12px;margin:10px 0}
    .row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
    input,select,button,textarea{
      padding:9px 10px;border-radius:10px;border:1px solid #ccc;font-size:14px
    }
    input,select,textarea{background:#fff}
    button{border:0;background:#1f6feb;color:#fff;cursor:pointer}
    button.secondary{background:#6b7280}
    button.danger{background:#dc2626}
    button.warn{background:#f59e0b;color:#111}
    small{color:#666}
    pre{
      background:#0b1020;color:#b7ffb7;padding:12px;border-radius:12px;
      height:380px;overflow:auto;margin:0
    }
    .w-120{width:120px}
    .w-160{width:160px}
    .w-200{width:200px}
    .w-260{width:260px}
    .w-320{width:320px}
    .w-420{width:420px}
    .w-520{width:520px}
    .badge{
      display:inline-block;padding:4px 8px;border-radius:999px;
      background:#eef2ff;color:#1f3cff;font-size:12px;border:1px solid #dbe2ff
    }
    .hint{background:#fff7ed;border:1px solid #fed7aa;color:#9a3412;padding:10px;border-radius:12px}
  </style>
</head>
<body>

  <h2>✅ Socket User Full Test (Blade)</h2>
  <div class="hint">
    <div><b>الهدف:</b> تتأكد الربط شغال + تشوف الأحداث لحظياً + تختبر nearby + التفاوض.</div>
    <div><b>الترتيب المقترح:</b> <span class="badge">Connect</span> ثم <span class="badge">user:loginInfo</span> (اختياري) ثم <span class="badge">user:findNearbyDrivers</span> ثم <span class="badge">Join Ride Room</span>.</div>
    <div><small>إذا ما ظهر event عندك بالواجهة بينما ظاهر بالتيرمنال، غالباً مشكلة listener بالفرونت أو الصفحة ما عملت join للغرفة الصحيحة.</small></div>
  </div>

  <div class="card">
    <div class="row">
      <input id="socketUrl" class="w-320" value="https://aiactive.co.uk:4000" placeholder="Socket URL">
      <button onclick="connect()">Connect</button>
      <button class="secondary" onclick="disconnect()">Disconnect</button>
      <span id="status" class="badge">disconnected</span>
    </div>
  </div>

  <div class="card">
    <h3 style="margin:0 0 8px 0">👤 بيانات المستخدم (user:loginInfo)</h3>
    <div class="row">
      <input id="userId" class="w-160" value="2" placeholder="user_id">
      <input id="userName" class="w-200" placeholder="user_name">
      <input id="userPhone" class="w-200" placeholder="contact_number">
      <input id="userCountry" class="w-160" placeholder="select_country_code">
      <input id="userImage" class="w-260" placeholder="profile_image (اختياري)">
      <button onclick="sendLoginInfo()">Send user:loginInfo</button>
    </div>
    <small>اختياري: يخزن بيانات المستخدم في الذاكرة ليستفاد منها في bidding.</small>
  </div>

  <div class="card">
    <h3 style="margin:0 0 8px 0">📡 Nearby (user:findNearbyDrivers)</h3>
    <div class="row">
      <input id="nearbyLat" class="w-160" value="33.5138" placeholder="lat">
      <input id="nearbyLong" class="w-160" value="36.2765" placeholder="long">
      <input id="nearbyServiceType" class="w-160" placeholder="service_type_id (اختياري)">
      <button onclick="startNearby()">Start Nearby</button>
      <button class="secondary" onclick="updateNearbyCenter()">Update Center</button>
      <button class="warn" onclick="setNearbyServiceType()">Set Service Type</button>
      <button class="danger" onclick="stopNearby()">Stop Nearby</button>
    </div>
    <div class="row" style="margin-top:8px">
      <button class="secondary" onclick="getNearbyVehicleTypes()">Get Vehicle Types (مرة واحدة)</button>
    </div>
    <small>
      يبدأ بث الأحداث: <b>user:nearbyDrivers</b> + <b>user:nearbyDrivers:update</b> + <b>user:nearbyVehicleTypes</b> + <b>user:nearbyVehicleTypes:update</b>.
    </small>
  </div>

  <div class="card">
    <h3 style="margin:0 0 8px 0">🎯 غرفة الرحلة (user:joinRideRoom)</h3>
    <div class="row">
      <input id="rideId" class="w-160" value="225" placeholder="ride_id">
      <button onclick="joinRide()">Join Ride Room</button>
      <button class="secondary" onclick="leaveRide()">Leave Ride Room</button>
    </div>
  </div>

  <div class="card">
    <h3 style="margin:0 0 8px 0">🚀 Dispatch داخلي (اختياري للتجربة)</h3>
    <div class="row">
      <input id="dispatchLat" class="w-160" value="33.5138" placeholder="pickup_lat">
      <input id="dispatchLong" class="w-160" value="36.2765" placeholder="pickup_long">
      <input id="dispatchRadius" class="w-160" value="5000" placeholder="radius (m)">
      <input id="dispatchUserBid" class="w-160" value="5000" placeholder="user_bid_price">
      <input id="dispatchMinFare" class="w-160" value="4000" placeholder="min_fare_amount">
      <input id="dispatchServiceType" class="w-160" placeholder="service_type_id (اختياري)">
      <button onclick="dispatchRide()">Emit ride:dispatchToNearbyDrivers</button>
    </div>
    <small>هذا Event داخلي تستعمله Laravel عادة. مفيد للاختبار السريع إذا ما عندك Laravel شغال.</small>
  </div>

  <div class="card">
    <h3 style="margin:0 0 8px 0">📩 تعديل السعر لكل السائقين (user:respondToDriver)</h3>
    <div class="row">
      <input id="offerPrice" class="w-160" placeholder="new_price (بيتعبي تلقائي من ride:newBid)">
      <button class="warn" onclick="respondToDriver()">Send user:respondToDriver</button>
    </div>
    <small>
      يبعت فقط <b>ride_id</b> + <b>price</b> لكل السائقين القريبين.
    </small>
  </div>

  <div class="card">
    <h3 style="margin:0 0 8px 0">✅ قبول العرض النهائي (user:acceptOffer)</h3>
    <div class="row">
      <input id="driverId" class="w-160" placeholder="driver_id (بيتعبي تلقائي)">
      <button onclick="acceptOffer()">Send user:acceptOffer</button>
    </div>
    <small>
      يعتمد على <b>ride_id</b> + <b>driver_id</b> + <b>offered_price</b> (بيتعبي غالباً من <b>ride:newBid</b>).
    </small>
  </div>

  <div class="card">
    <h3 style="margin:0 0 8px 0">🧨 إنهاء/إلغاء الرحلة</h3>
    <div class="row">
      <button class="danger" onclick="cancelRide()">Send ride:cancel</button>
      <button class="secondary" onclick="closeRide()">Send ride:close</button>
    </div>
    <small>
      للإلغاء: السيرفر يبث <b>ride:cancelled</b>. للإغلاق: <b>ride:closed</b>.
    </small>
  </div>

  <div class="card">
    <div class="row" style="justify-content:space-between;align-items:flex-start">
      <div style="flex:1">
        <h3 style="margin:0 0 8px 0">📡 Live Log</h3>
        <small>بتشوف كل الأحداث اللي واصلة للصفحة + payload.</small>
      </div>
      <div class="row">
        <button class="secondary" onclick="clearLog()">Clear</button>
      </div>
    </div>
    <pre id="log"></pre>
  </div>

  <!-- لازم يكون من نفس سيرفر السوكيت -->
  <script src="https://aiactive.co.uk:4000/socket.io/socket.io.js"></script>

  <script>
    let socket = null;
    let currentRideId = null;

    function setStatus(txt){
      const el = document.getElementById("status");
      el.textContent = txt;
    }

    function log(...args){
      const el = document.getElementById('log');
      const line = args.map(a => typeof a === "string" ? a : JSON.stringify(a, null, 2)).join(" ");
      el.textContent += line + "\n";
      el.scrollTop = el.scrollHeight;
      console.log(...args);
    }

    function clearLog(){
      document.getElementById('log').textContent = "";
    }

    function n(v){
      const x = Number(v);
      return Number.isFinite(x) ? x : null;
    }

    function connect(){
      const url = document.getElementById('socketUrl').value.trim();
      if (!url) return log("⚠️ socketUrl فاضي");

      if (socket) socket.disconnect();

      socket = io(url, { transports: ["websocket"] });

      socket.on("connect", () => {
        setStatus("connected: " + socket.id);
        log("✅ CONNECTED", { id: socket.id });
      });

      socket.on("disconnect", (reason) => {
        setStatus("disconnected");
        log("❌ DISCONNECTED", reason);
      });

      // ✅ onAny: اطبع أي حدث بيوصل
      socket.onAny((event, ...args) => {
        log("📩 [onAny]", event, args);
      });

      // ✅ listeners الأساسية
      socket.on("ride:joined", (p) => log("📌 ride:joined", p));
      socket.on("ride:newBid", (p) => {
        const bidTime = p?.bidding_time ?? null;
        const bidReadable = bidTime ? new Date(bidTime).toLocaleString() : null;
        log("💰 ride:newBid (from driver)", { ...p, bidding_time_readable: bidReadable });

        if (p?.driver_id != null) document.getElementById("driverId").value = p.driver_id;
        if (p?.offered_price != null) document.getElementById("offerPrice").value = p.offered_price;
        if (p?.user_bid_price_final != null) document.getElementById("offerPrice").value = p.user_bid_price_final;
      });

      socket.on("user:nearbyDrivers", (p) => {
        const drivers = Array.isArray(p) ? p : (p?.drivers ?? []);
        const radius = Array.isArray(p) ? null : (p?.radius ?? null);
        log("📍 user:nearbyDrivers", { count: drivers.length, radius, drivers });
      });
      socket.on("user:nearbyDrivers:update", (p) => {
        const drivers = Array.isArray(p) ? p : (p?.drivers ?? []);
        const radius = Array.isArray(p) ? null : (p?.radius ?? null);
        log("🔁 user:nearbyDrivers:update", { count: drivers.length, radius, drivers });
      });
      socket.on("user:nearbyVehicleTypes", (p) => log("🚗 user:nearbyVehicleTypes", p));
      socket.on("user:nearbyVehicleTypes:update", (p) => log("♻️ user:nearbyVehicleTypes:update", p));

      socket.on("ride:locationUpdate", (p) => log("🛰️ ride:locationUpdate", p));
      socket.on("ride:statusUpdated", (p) => log("📣 ride:statusUpdated", p));
      socket.on("ride:arrived", (p) => log("🟡 ride:arrived", p));
      socket.on("ride:ended", (p) => log("🏁 ride:ended", p));

      socket.on("ride:userAccepted", (p) => {
        const atReadable = p?.at ? new Date(p.at).toLocaleString() : null;
        log("✅ ride:userAccepted", { ...p, at_readable: atReadable });

        if (p?.driver_id != null) document.getElementById("driverId").value = p.driver_id;
        if (p?.offered_price != null) document.getElementById("offerPrice").value = p.offered_price;
      });
      socket.on("ride:closed", (p) => log("🔒 ride:closed", p));
      socket.on("ride:cancelled", (p) => log("🧨 ride:cancelled", p));
    }

    function disconnect(){
      if (!socket) return;
      socket.disconnect();
      socket = null;
      currentRideId = null;
      setStatus("disconnected");
    }

    function sendLoginInfo(){
      if (!socket) return log("⚠️ Connect أولاً");

      const user_id = n(document.getElementById('userId').value);
      if (!user_id) return log("⚠️ user_id غلط");

      const payload = {
        user_id,
        user_name: document.getElementById('userName').value || null,
        contact_number: document.getElementById('userPhone').value || null,
        select_country_code: document.getElementById('userCountry').value || null,
        profile_image: document.getElementById('userImage').value || null,
      };

      socket.emit("user:loginInfo", payload);
      log("➡️ emit user:loginInfo", payload);
    }

    function startNearby(){
      if (!socket) return log("⚠️ Connect أولاً");

      const user_id = n(document.getElementById('userId').value);
      const lat = n(document.getElementById('nearbyLat').value);
      const long = n(document.getElementById('nearbyLong').value);
      const service_type_id = n(document.getElementById('nearbyServiceType').value);

      if (!user_id || lat === null || long === null) {
        return log("⚠️ لازم user_id + lat + long");
      }

      const payload = { user_id, lat, long };
      if (service_type_id !== null) payload.service_type_id = service_type_id;

      socket.emit("user:findNearbyDrivers", payload);
      log("➡️ emit user:findNearbyDrivers", payload);
    }

    function updateNearbyCenter(){
      if (!socket) return log("⚠️ Connect أولاً");

      const lat = n(document.getElementById('nearbyLat').value);
      const long = n(document.getElementById('nearbyLong').value);
      if (lat === null || long === null) return log("⚠️ لازم lat + long");

      const payload = { lat, long };
      socket.emit("user:updateNearbyCenter", payload);
      log("➡️ emit user:updateNearbyCenter", payload);
    }

    function setNearbyServiceType(){
      if (!socket) return log("⚠️ Connect أولاً");
      const service_type_id = n(document.getElementById('nearbyServiceType').value);
      const payload = { service_type_id: service_type_id === null ? null : service_type_id };
      socket.emit("user:setNearbyServiceType", payload);
      log("➡️ emit user:setNearbyServiceType", payload);
    }

    function stopNearby(){
      if (!socket) return log("⚠️ Connect أولاً");
      socket.emit("user:stopNearbyDrivers");
      log("➡️ emit user:stopNearbyDrivers");
    }

    function getNearbyVehicleTypes(){
      if (!socket) return log("⚠️ Connect أولاً");
      const lat = n(document.getElementById('nearbyLat').value);
      const long = n(document.getElementById('nearbyLong').value);
      if (lat === null || long === null) return log("⚠️ لازم lat + long");

      const payload = { lat, long };
      socket.emit("user:getNearbyVehicleTypes", payload);
      log("➡️ emit user:getNearbyVehicleTypes", payload);
    }

    function joinRide(){
      if (!socket) return log("⚠️ Connect أولاً");

      const user_id = n(document.getElementById('userId').value);
      const ride_id = n(document.getElementById('rideId').value);

      if (!user_id || !ride_id) return log("⚠️ user_id أو ride_id غلط");

      currentRideId = ride_id;
      socket.emit("user:joinRideRoom", { user_id, ride_id });
      log("➡️ emit user:joinRideRoom", { user_id, ride_id });
    }

    function leaveRide(){
      currentRideId = null;
      log("ℹ️ leaveRide: لا يوجد handler بالسيرفر حالياً — استخدم Disconnect أو Join لRide أخرى");
    }

    function dispatchRide(){
      if (!socket) return log("⚠️ Connect أولاً");

      const ride_id = n(document.getElementById('rideId').value);
      const user_id = n(document.getElementById('userId').value);
      const pickup_lat = n(document.getElementById('dispatchLat').value);
      const pickup_long = n(document.getElementById('dispatchLong').value);
      const radius = n(document.getElementById('dispatchRadius').value);
      const user_bid_price = n(document.getElementById('dispatchUserBid').value);
      const min_fare_amount = n(document.getElementById('dispatchMinFare').value);
      const service_type_id = n(document.getElementById('dispatchServiceType').value);

      if (!ride_id || pickup_lat === null || pickup_long === null) {
        return log("⚠️ لازم ride_id + pickup_lat + pickup_long");
      }

      const payload = {
        ride_id,
        pickup_lat,
        pickup_long,
        radius: radius ?? 5000,
        user_bid_price: user_bid_price ?? null,
        min_fare_amount: min_fare_amount ?? null,
        user_id: user_id ?? null,
      };
      if (service_type_id !== null) payload.service_type_id = service_type_id;

      socket.emit("ride:dispatchToNearbyDrivers", payload);
      log("➡️ emit ride:dispatchToNearbyDrivers", payload);
    }

    function respondToDriver(){
      if (!socket) return log("⚠️ Connect أولاً");

      const ride_id = n(document.getElementById('rideId').value);
      const price = n(document.getElementById('offerPrice').value);

      if (!ride_id || price === null) {
        return log("⚠️ لازم ride_id + price");
      }

      socket.emit("user:respondToDriver", {
        ride_id,
        price
      });

      log("➡️ emit user:respondToDriver", { ride_id, price });
    }

    function acceptOffer(){
      if (!socket) return log("⚠️ Connect أولاً");

      const ride_id = n(document.getElementById('rideId').value);
      const driver_id = n(document.getElementById('driverId').value);
      const offered_price = n(document.getElementById('offerPrice').value);

      if (!ride_id || !driver_id || offered_price === null) {
        return log("⚠️ لازم ride_id + driver_id + offered_price");
      }

      socket.emit("user:acceptOffer", { ride_id, driver_id, offered_price });
      log("➡️ emit user:acceptOffer", { ride_id, driver_id, offered_price });
    }

    function closeRide(){
      if (!socket) return log("⚠️ Connect أولاً");
      const ride_id = n(document.getElementById('rideId').value);
      if (!ride_id) return log("⚠️ ride_id غلط");
      socket.emit("ride:close", { ride_id });
      log("➡️ emit ride:close", { ride_id });
    }

    function cancelRide(){
      if (!socket) return log("⚠️ Connect أولاً");
      const ride_id = n(document.getElementById('rideId').value);
      const user_id = n(document.getElementById('userId').value);
      if (!ride_id) return log("⚠️ ride_id غلط");

      socket.emit("ride:cancel", { ride_id, user_id, reason: "test-cancel-from-blade" });
      log("➡️ emit ride:cancel", { ride_id, user_id, reason: "test-cancel-from-blade" });
    }
  </script>
</body>
</html>
