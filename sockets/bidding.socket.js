// sockets/bidding.socket.js
const driverLocationService = require("../services/driverLocation.service");
const axios = require("axios"); // لضمان استدعاء Laravel API عند قبول العرض
const { getDistanceMeters } = require("../utils/geo.util");
const { getUserDetails, getUserDetailsByToken } = require("../store/users.store");
const {
  clearActiveRideByRideId,
  getActiveRideByDriver,
  getActiveDriverByRide,
  setActiveRide,
  clearActiveRideByDriver,
} = require("../store/activeRides.store");

const DEBUG_EVENTS = process.env.DEBUG_SOCKET_EVENTS === "1";
const debugLog = (event, payload, socketId) => {
  if (!DEBUG_EVENTS) return;
  console.log("[bidding.socket]", event, "socket:", socketId, "payload:", payload);
};

// ✅ In-memory Maps (no Redis)
const rideCandidates = new Map(); // rideId -> Set(driverId)
const driverRideInbox = new Map(); // driverId -> Map(rideId -> ridePayload)
const driverLastBidStatus = new Map(); // driverId -> { rideId, responded }
const acceptLocks = new Map(); // rideId -> timestamp (prevent double-accept race)

// ✅ NEW: per-driver patch sequence (ordering)
const driverPatchSeq = new Map(); // driverId -> number

const ACCEPT_LOCK_TTL_MS = Number.isFinite(Number(process.env.ACCEPT_LOCK_TTL_MS))
  ? Number(process.env.ACCEPT_LOCK_TTL_MS)
  : 5000;

const rideRoom = (rideId) => `ride:${rideId}`;
const driverRoom = (driverId) => `driver:${driverId}`;

const toNumber = (v) => {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

const round2 = (v) => (Number.isFinite(v) ? Math.round(v * 100) / 100 : null);
const DRIVER_TO_PICKUP_SPEED_KMPH = Number.isFinite(
  Number(process.env.DRIVER_TO_PICKUP_SPEED_KMPH)
)
  ? Math.max(5, Number(process.env.DRIVER_TO_PICKUP_SPEED_KMPH))
  : 28;

// ✅ UPDATED: timeout صار 90 ثانية (ALL TIMER VALUES RETURNED IN SECONDS)
const RIDE_TIMEOUT_S = Number.isFinite(Number(process.env.RIDE_TIMEOUT_S))
  ? Number(process.env.RIDE_TIMEOUT_S)
  : 90; // ✅ fixed 90 seconds

const CANCELLED_RIDE_TTL_MS = 10 * 60 * 1000;

// ✅ NEW: TTL لتنظيف الانبوكس من الرحلات القديمة (حتى لو ما وصل cancel/accept)
const INBOX_ENTRY_TTL_MS = Number.isFinite(Number(process.env.INBOX_ENTRY_TTL_MS))
  ? Number(process.env.INBOX_ENTRY_TTL_MS)
  : 2 * 60 * 1000; // default 2 minutes

const DEFAULT_RADIUS_METERS = 5000;
const MIN_DISPATCH_RADIUS_METERS = 200;
const MAX_DISPATCH_RADIUS_METERS = Number.isFinite(Number(process.env.MAX_DISPATCH_RADIUS_METERS))
  ? Number(process.env.MAX_DISPATCH_RADIUS_METERS)
  : 7000;

const MAX_DISPATCH_CANDIDATES = Number.isFinite(Number(process.env.MAX_DISPATCH_CANDIDATES))
  ? Number(process.env.MAX_DISPATCH_CANDIDATES)
  : 30;

const MAX_DRIVER_LOCATION_AGE_MS = Number.isFinite(Number(process.env.MAX_DRIVER_LOCATION_AGE_MS))
  ? Number(process.env.MAX_DRIVER_LOCATION_AGE_MS)
  : 2 * 60 * 1000;

const LARAVEL_BASE_URL =
  process.env.LARAVEL_BASE_URL ||
  process.env.LARAVEL_URL ||
  "https://aiactive.co.uk/backend/backend-laravel/public";

const LARAVEL_ACCEPT_BID_PATH = "/api/customer/transport/accept-bid";
const LARAVEL_DRIVER_BID_PATH = "/api/driver/bid-offer";
const LARAVEL_ACCEPT_BID_TIMEOUT_MS = Number.isFinite(
  Number(process.env.LARAVEL_ACCEPT_BID_TIMEOUT_MS)
)
  ? Number(process.env.LARAVEL_ACCEPT_BID_TIMEOUT_MS)
  : 7000;

// ─────────────────────────────
// ✅ Timer helpers (SECONDS ONLY + SERVER AUTHORITATIVE)
// ─────────────────────────────
/**
 * ✅ Timer payload (server-authoritative) — ALL IN SECONDS
 * - server_time: epoch seconds
 * - expires_at: epoch seconds
 * - timeout_ms: duration seconds (kept key name to avoid breaking old clients)
 */
const nowSec = () => Math.floor(Date.now() / 1000);

const makeTimer = (durationSec = RIDE_TIMEOUT_S) => {
  const now = nowSec();
  const durSec = Math.max(0, Math.floor(Number.isFinite(Number(durationSec)) ? Number(durationSec) : RIDE_TIMEOUT_S));
  return {
    server_time: now,          // seconds
    expires_at: now + durSec,  // seconds
    timeout_ms: durSec,        // seconds (kept key name)
  };
};

// ─────────────────────────────
// ✅ Patch system helpers
// ─────────────────────────────
const nextDriverSeq = (driverId) => {
  const cur = driverPatchSeq.get(driverId) || 0;
  const next = cur + 1;
  driverPatchSeq.set(driverId, next);
  return next;
};

/**
 * ✅ Patch event (delta) — no full inbox resend
 * ops:
 *  - { op: "upsert", ride: <ridePayload> }
 *  - { op: "remove", ride_id: <id> }
 */
function emitDriverPatch(io, driverId, ops = []) {
  if (!driverId || !ops || ops.length === 0) return;
  const safeOps = ops.map((op) => {
    if (!op || typeof op !== "object") return op;
    if (op.op !== "upsert" || !op.ride || typeof op.ride !== "object") return op;
    return {
      ...op,
      ride: sanitizeRidePayloadForClient(op.ride),
    };
  });
  io.to(driverRoom(driverId)).emit("driver:rides:patch", {
    driver_id: driverId,
    ops: safeOps,
    seq: nextDriverSeq(driverId),
    at: Date.now(),
  });
}

const fetchDriverMetaFromApi = async (driverId, accessToken, driverServiceId) => {
  if (!driverId || !accessToken || !driverServiceId) return null;
  try {
    const res = await axios.post(
      `${LARAVEL_BASE_URL}/api/driver/update-current-status`,
      {
        driver_id: driverId,
        access_token: accessToken,
        driver_service_id: driverServiceId,
        update_status: 1,
      },
      { timeout: 7000 }
    );

    let d = res?.data || {};
    if (typeof d === "string") {
      try {
        d = JSON.parse(d);
      } catch (_) {}
    }

    const driverName = d.driver_name ?? null;
    const vehicleType = d.vehicle_type_name ?? null;
    const vehicleNumber = d.plat_no ?? null;
    const rating = d.rating ?? null;
    const driverImage =
      d.driver_image ?? d.profile_image ?? d.driver_image_url ?? d.avatar ?? d.image ?? null;

    const metaUpdate = {
      ...(driverName ? { driver_name: driverName } : {}),
      ...(vehicleType ? { vehicle_type_name: vehicleType } : {}),
      ...(vehicleNumber ? { plat_no: vehicleNumber } : {}),
      ...(rating != null ? { rating } : {}),
      ...(driverImage ? { driver_image: driverImage } : {}),
    };

    if (Object.keys(metaUpdate).length > 0) {
      driverLocationService.updateMeta(driverId, metaUpdate);
    }

    return {
      driver_name: driverName,
      vehicle_type: vehicleType,
      vehicle_number: vehicleNumber,
      rating,
      driver_image: driverImage,
    };
  } catch (e) {
    return null;
  }
};

const isDriverDetailsEmpty = (d) => {
  if (!d) return true;
  return (
    d.driver_name == null &&
    d.vehicle_type == null &&
    d.vehicle_number == null &&
    d.rating == null &&
    d.driver_image == null
  );
};

// User -> ride owner map (rideId -> userId)
const rideOwnerByRide = new Map(); // rideId -> userId

const getUserIdFromDispatch = (data, userDetails) => {
  const uid = toNumber(
    data?.user_id ?? data?.customer_id ?? data?.passenger_id ?? userDetails?.user_id
  );
  return uid || null;
};

const setUserActiveRide = (userId, rideId) => {
  if (!userId || !rideId) return;
  rideOwnerByRide.set(rideId, userId);
};

const clearUserRideByRideId = (rideId) => {
  if (!rideId) return;
  rideOwnerByRide.delete(rideId);
};

const getUserIdForRide = (rideId) => {
  if (!rideId) return null;
  return rideOwnerByRide.get(rideId) ?? null;
};

// ✅ NEW: unify token extraction (fix retry cases where token name differs)
const getTokenFromAny = (data, built = null, src = null) => {
  return (
    data?.token ??
    data?.access_token ??
    data?.user_token ??
    data?.user_details?.user_token ??
    built?.user_token ??
    src?.user_token ??
    src?.token ??
    null
  );
};

const buildUserDetails = (data) => {
  const src =
    (data?.user_details && typeof data.user_details === "object" && data.user_details) ||
    (data?.user && typeof data.user === "object" && data.user) ||
    (data?.customer && typeof data.customer === "object" && data.customer) ||
    null;

  let userId = toNumber(
    src?.user_id ?? src?.id ?? src?.customer_id ?? data?.user_id ?? data?.customer_id
  );

  // ✅ NEW: token fallback (supports retry payloads using access_token / user_token / user_details.user_token)
  const token = getTokenFromAny(data, null, src);

  const userName =
    src?.user_name ??
    src?.name ??
    src?.customer_name ??
    data?.user_name ??
    data?.customer_name ??
    null;

  const genderRaw = src?.gender ?? src?.user_gender ?? data?.gender ?? data?.user_gender ?? null;

  const userGender =
    genderRaw === "" || genderRaw == null
      ? null
      : Number.isFinite(Number(genderRaw))
      ? Number(genderRaw)
      : genderRaw;

  const countryCode =
    src?.select_country_code ?? data?.select_country_code ?? src?.country_code ?? null;

  const contactNumber =
    src?.contact_number ??
    src?.user_phone ??
    src?.phone ??
    src?.mobile ??
    data?.contact_number ??
    data?.user_phone ??
    data?.customer_phone ??
    data?.phone ??
    null;

  const userImage =
    src?.profile_image ??
    src?.user_image ??
    src?.image ??
    src?.avatar ??
    data?.profile_image ??
    data?.user_image ??
    data?.customer_image ??
    null;

  const stored = userId ? getUserDetails(userId) : null;
  const storedByToken = !stored && token ? getUserDetailsByToken(token) : null;

  if (!userId && storedByToken?.user_id) {
    userId = toNumber(storedByToken.user_id) ?? userId;
  }

  const details = {
    user_id: userId ?? stored?.user_id ?? storedByToken?.user_id ?? null,
    user_name: userName ?? stored?.user_name ?? storedByToken?.user_name ?? null,
    user_gender: userGender ?? stored?.user_gender ?? storedByToken?.user_gender ?? null,
    user_phone: contactNumber ?? stored?.user_phone ?? storedByToken?.user_phone ?? null,
    user_country_code:
      countryCode ?? stored?.user_country_code ?? storedByToken?.user_country_code ?? null,
    user_phone_full:
      contactNumber && countryCode
        ? `${countryCode}${contactNumber}`
        : stored?.user_phone_full ?? storedByToken?.user_phone_full ?? null,
    user_image: userImage ?? stored?.user_image ?? storedByToken?.user_image ?? null,

    // ✅ NEW: keep token in user_details snapshot (helps later merges on retry)
    user_token: token ?? stored?.user_token ?? stored?.token ?? storedByToken?.user_token ?? null,
    token: token ?? stored?.user_token ?? stored?.token ?? storedByToken?.user_token ?? null,
  };

  if (
    !details.user_id &&
    !details.user_name &&
    !details.user_phone &&
    !details.user_image &&
    !details.user_token
  ) {
    return null;
  }
  return details;
};

const buildCustomerPayload = (payload = {}, userDetails = null) => {
  const details =
    userDetails && typeof userDetails === "object"
      ? userDetails
      : payload?.user_details && typeof payload.user_details === "object"
      ? payload.user_details
      : null;

  const customerId = toNumber(
    details?.user_id ??
      payload?.user_id ??
      payload?.customer_id ??
      payload?.passenger_id ??
      payload?.user_details?.user_id
  );

  const customerName =
    details?.user_name ??
    payload?.user_name ??
    payload?.customer_name ??
    payload?.passenger_name ??
    null;

  const customerGenderRaw =
    details?.user_gender ??
    payload?.user_gender ??
    payload?.customer_gender ??
    payload?.gender ??
    null;
  const customerGender =
    customerGenderRaw === "" || customerGenderRaw == null
      ? null
      : Number.isFinite(Number(customerGenderRaw))
      ? Number(customerGenderRaw)
      : customerGenderRaw;

  const customerCountryCode =
    details?.user_country_code ??
    payload?.user_country_code ??
    payload?.customer_country_code ??
    payload?.country_code ??
    payload?.select_country_code ??
    null;

  const customerPhone =
    details?.user_phone ??
    payload?.user_phone ??
    payload?.customer_phone ??
    payload?.contact_number ??
    payload?.phone ??
    null;

  const customerPhoneFull =
    details?.user_phone_full ??
    payload?.user_phone_full ??
    payload?.customer_phone_full ??
    (customerCountryCode && customerPhone ? `${customerCountryCode}${customerPhone}` : null);

  const customerImage =
    details?.user_image ??
    payload?.user_image ??
    payload?.customer_image ??
    payload?.profile_image ??
    payload?.avatar ??
    null;

  const customerToken =
    details?.user_token ??
    details?.token ??
    payload?.user_token ??
    payload?.token ??
    payload?.access_token ??
    null;

  if (
    !customerId &&
    !customerName &&
    !customerPhone &&
    !customerImage &&
    !customerToken &&
    !details
  ) {
    return null;
  }

  return {
    user_id: customerId ?? null,
    user_name: customerName ?? null,
    user_gender: customerGender ?? null,
    user_country_code: customerCountryCode ?? null,
    user_phone: customerPhone ?? null,
    user_phone_full: customerPhoneFull ?? null,
    user_image: customerImage ?? null,
    user_token: customerToken ?? null,
    token: customerToken ?? null,
  };
};

const attachCustomerFields = (payload = {}, userDetails = null) => {
  if (!payload || typeof payload !== "object") return payload;

  const customer = buildCustomerPayload(payload, userDetails);
  return {
    ...payload,
    customer: customer ?? null,
    customer_details: customer ?? null,
    customer_id: customer?.user_id ?? null,
    customer_name: customer?.user_name ?? null,
    customer_gender: customer?.user_gender ?? null,
    customer_country_code: customer?.user_country_code ?? null,
    customer_phone: customer?.user_phone ?? null,
    customer_phone_full: customer?.user_phone_full ?? null,
    customer_image: customer?.user_image ?? null,
  };
};

const sanitizeRidePayloadForClient = (payload = {}) => {
  if (!payload || typeof payload !== "object") return payload;
  const {
    user_details,
    customer,
    customer_details,
    customer_id,
    customer_name,
    customer_gender,
    customer_country_code,
    customer_phone,
    customer_phone_full,
    customer_image,
    ...rest
  } = payload;
  return rest;
};

// ✅ rides cancelled (block dispatch)
const cancelledRides = new Set(); // rideId

// ─────────────────────────────
// Inbox helpers
// ─────────────────────────────
const rideTimers = new Map(); // rideId -> setTimeout ID

// الخريطة لحفظ تفاصيل الرحلات في الذاكرة (إذا احتجتها لاحقاً)
const rideDetailsMap = new Map();
function saveRideDetails(rideId, rideDetails) {
  rideDetailsMap.set(rideId, rideDetails);
  console.log(`Details for ride ${rideId} saved in memory.`);
}
function getRideDetails(rideId) {
  return rideDetailsMap.get(rideId);
}

function isAcceptLocked(rideId) {
  if (!rideId) return false;
  const now = Date.now();
  const last = acceptLocks.get(rideId);
  if (last && now - last < ACCEPT_LOCK_TTL_MS) {
    return true;
  }
  acceptLocks.set(rideId, now);
  setTimeout(() => {
    if (acceptLocks.get(rideId) === now) acceptLocks.delete(rideId);
  }, ACCEPT_LOCK_TTL_MS);
  return false;
}

function finalizeAcceptedRide(io, rideId, driverId, finalPrice, options = {}) {
  const { message = "User accepted the offer", rideDetails = null } = options || {};

  if (isAcceptLocked(rideId)) {
    console.log(`⚠️ accept dedupe: ride ${rideId} already processed recently`);
    return;
  }
  const activeDriverId = getActiveDriverByRide(rideId);
  if (activeDriverId && activeDriverId === driverId) {
    console.log(`ℹ️ ride ${rideId} already accepted by driver ${driverId}`);
    return;
  }
  if (activeDriverId && activeDriverId !== driverId) {
    console.log(
      `⚠️ ride ${rideId} already accepted by driver ${activeDriverId}, ignoring accept from ${driverId}`
    );
    return;
  }

  clearActiveRideByDriver(driverId);
  setActiveRide(driverId, rideId);

  const snapshot =
    rideDetails ?? (typeof getRideDetails === "function" ? getRideDetails(rideId) : null);
  const routeKm = toNumber(snapshot?.route ?? snapshot?.meta?.route ?? null);
  const etaRaw = toNumber(snapshot?.eta_min ?? snapshot?.meta?.eta_min ?? null);
  const etaMin = etaRaw !== null ? round2(etaRaw / 60) : null;

  const rideDetailsPayload = snapshot
    ? (() => {
        const cleanedMeta = snapshot.meta ? { ...snapshot.meta } : null;
        if (cleanedMeta) {
          delete cleanedMeta.route;
          delete cleanedMeta.eta_min;
        }
        return {
          ...snapshot,
          ...(routeKm !== null ? { route: routeKm } : {}),
          ...(etaMin !== null ? { eta_min: etaMin } : {}),
          ...(cleanedMeta ? { meta: cleanedMeta } : {}),
        };
      })()
    : routeKm !== null || etaMin !== null
    ? {
        ...(routeKm !== null ? { route: routeKm } : {}),
        ...(etaMin !== null ? { eta_min: etaMin } : {}),
      }
    : null;

  const acceptedPayload = {
    ride_id: rideId,
    driver_id: driverId,
    offered_price: finalPrice,
    message,
    at: Date.now(),
  };
  if (rideDetailsPayload) acceptedPayload.ride_details = rideDetailsPayload;

  io.to(driverRoom(driverId)).emit("ride:userAccepted", acceptedPayload);
  io.to(rideRoom(rideId)).emit("ride:userAccepted", acceptedPayload);
  io.to(rideRoom(rideId)).emit("ride:trackingStarted", {
    ride_id: rideId,
    driver_id: driverId,
    at: Date.now(),
  });
  closeRideBidding(io, rideId, { clearUser: false });

  const d = driverLocationService.getDriver(driverId);
  if (d?.lat != null && d?.long != null) {
    io.to(rideRoom(rideId)).emit("ride:locationUpdate", {
      ride_id: rideId,
      driver_id: driverId,
      lat: d.lat,
      long: d.long,
      at: Date.now(),
    });
  }

  console.log(`✅ ride accepted -> ride ${rideId} driver ${driverId} price ${finalPrice}`);
}

function cancelRideTimeout(rideId) {
  const timer = rideTimers.get(rideId);
  if (timer) {
    clearTimeout(timer);
    rideTimers.delete(rideId);
  }
}

/**
 * ✅ UPDATED: startRideTimeout accepts seconds (NOT ms)
 */
function startRideTimeout(io, rideId, durationSec = RIDE_TIMEOUT_S) {
  cancelRideTimeout(rideId);

  const sec = Math.max(0, toNumber(durationSec) ?? RIDE_TIMEOUT_S);
  const ms = sec * 1000;

  const timer = setTimeout(() => {
    console.log(`🛑 Ride ${rideId} has timed out. Removing from memory.`);
    // ✅ timeout -> remove from all inboxes + candidates
    removeRideFromAllInboxes(io, rideId);
    rideCandidates.delete(rideId);
  }, ms);

  rideTimers.set(rideId, timer);
}

/**
 * ✅ NEW: unified timer refresh (always 90s, server authoritative)
 */
function refreshRideTimer(io, rideId, options = {}) {
  const { update_snapshot = true, patch_inboxes = true } = options || {};
  if (!rideId) return null;

  const timer = makeTimer(RIDE_TIMEOUT_S); // ✅ seconds

  // restart real server timeout to match expires_at (seconds)
  startRideTimeout(io, rideId, Math.max(0, timer.expires_at - nowSec()));

  // update snapshot
  if (update_snapshot) {
    const snap = getRideDetails(rideId);
    if (snap && typeof snap === "object") {
      saveRideDetails(rideId, {
        ...snap,
        ...timer,
      });
    }
  }

  // update inbox entries
  if (patch_inboxes) {
    for (const [driverId, box] of driverRideInbox.entries()) {
      if (!box?.has?.(rideId)) continue;
      const current = box.get(rideId);
      if (!current || typeof current !== "object") continue;

      const updated = {
        ...current,
        ...timer,
        _ts: Date.now(),
      };
      const updatedWithCustomer = attachCustomerFields(updated, current?.user_details ?? null);

      box.set(rideId, updatedWithCustomer);
      emitDriverPatch(io, driverId, [{ op: "upsert", ride: updatedWithCustomer }]);
    }
  }

  return timer;
}

function inboxUpsert(driverId, rideId, payload) {
  if (!driverRideInbox.has(driverId)) driverRideInbox.set(driverId, new Map());
  const prepared = attachCustomerFields(payload, payload?.user_details ?? null);
  driverRideInbox.get(driverId).set(rideId, {
    ...prepared,
    ride_id: rideId,
    _ts: Date.now(), // ✅ مهم للتنظيف
  });
}

function inboxRemove(driverId, rideId) {
  const box = driverRideInbox.get(driverId);
  if (!box) return false;
  const existed = box.delete(rideId);
  if (box.size === 0) driverRideInbox.delete(driverId);
  return existed;
}

function clearDriverBidStatus(driverId, rideId) {
  const lastBid = driverLastBidStatus.get(driverId);
  if (lastBid && lastBid.rideId === rideId) {
    driverLastBidStatus.delete(driverId);
  }
}

// ✅ prune inbox entries by TTL (removes stale rides) + emit REMOVE patches
function pruneDriverInbox(io, driverId) {
  const box = driverRideInbox.get(driverId);
  if (!box) return;

  const now = Date.now();
  const removedOps = [];

  for (const [rideId, ride] of box.entries()) {
    const ts = toNumber(ride?._ts) ?? 0;
    if (ts && now - ts > INBOX_ENTRY_TTL_MS) {
      box.delete(rideId);
      clearDriverBidStatus(driverId, rideId);
      removedOps.push({ op: "remove", ride_id: rideId });
      console.log(`🧹 [inbox prune] removed old ride ${rideId} from driver ${driverId}`);
    }
  }

  if (box.size === 0) driverRideInbox.delete(driverId);

  if (removedOps.length > 0) {
    emitDriverPatch(io, driverId, removedOps);
    console.log(`🧹 [inbox prune] driver ${driverId}: removed ${removedOps.length} stale ride(s)`);
  }
}

// ✅ periodic global prune (handles idle memory leaks)
setInterval(() => {
  try {
    for (const driverId of driverRideInbox.keys()) {
      const box = driverRideInbox.get(driverId);
      if (!box) continue;
      const now = Date.now();
      for (const [rideId, ride] of box.entries()) {
        const ts = toNumber(ride?._ts) ?? 0;
        if (ts && now - ts > INBOX_ENTRY_TTL_MS) {
          box.delete(rideId);
          clearDriverBidStatus(driverId, rideId);
        }
      }
      if (box.size === 0) driverRideInbox.delete(driverId);
    }
  } catch (e) {
    console.log("⚠️ [inbox prune] interval error:", e?.message || e);
  }
}, 30 * 1000);

function inboxList(driverId, limit = 30) {
  const box = driverRideInbox.get(driverId);
  if (!box) return [];
  return Array.from(box.values())
    .sort((a, b) => (b._ts || 0) - (a._ts || 0))
    .slice(0, limit);
}

function getFullRideSnapshot(rideId, driverId = null) {
  const fromMap = getRideDetails(rideId);
  if (fromMap) return fromMap;

  if (driverId) {
    const fromDriverInbox = driverRideInbox.get(driverId)?.get(rideId);
    if (fromDriverInbox) return fromDriverInbox;
  }

  return findRideInInboxes(rideId);
}

// ✅ Snapshot ONLY (used on driver:getRidesList)
function emitDriverInbox(io, driverId, eventName = "driver:rides:list") {
  pruneDriverInbox(io, driverId);

  const list = inboxList(driverId, 30).map((ride) => sanitizeRidePayloadForClient(ride));
  io.to(driverRoom(driverId)).emit(eventName, {
    driver_id: driverId,
    rides: list,
    total: list.length,
    at: Date.now(),
  });
}

function updateRideUserDetailsInInbox(io, rideId, userDetails) {
  if (!rideId || !userDetails) return;

  const next = {
    user_id: userDetails.user_id ?? null,
    user_name: userDetails.user_name ?? null,
    user_gender: userDetails.user_gender ?? null,
    user_image: userDetails.user_image ?? null,

    // (اختياري) لو بدك كمان ينعكس بسرعة بالـ patch
    user_phone: userDetails.user_phone ?? null,
    user_country_code: userDetails.user_country_code ?? null,
    user_phone_full: userDetails.user_phone_full ?? null,
    user_token: userDetails.user_token ?? userDetails.token ?? null,
  };

  for (const [driverId, box] of driverRideInbox.entries()) {
    const ride = box.get(rideId);
    if (!ride) continue;

    const updated = attachCustomerFields({
      ...ride,
      user_id: next.user_id ?? ride.user_id ?? null,
      user_name: next.user_name ?? ride.user_name ?? null,
      user_gender: next.user_gender ?? ride.user_gender ?? null,
      user_image: next.user_image ?? ride.user_image ?? null,

      user_phone: next.user_phone ?? ride.user_phone ?? null,
      user_country_code: next.user_country_code ?? ride.user_country_code ?? null,
      user_phone_full: next.user_phone_full ?? ride.user_phone_full ?? null,

      token: next.user_token ?? ride.token ?? null,
      user_details: {
        ...(ride.user_details && typeof ride.user_details === "object" ? ride.user_details : {}),
        ...userDetails,
        user_token: next.user_token ?? userDetails?.user_token ?? userDetails?.token ?? null,
        token: next.user_token ?? userDetails?.user_token ?? userDetails?.token ?? null,
      },

      _ts: Date.now(),
    }, userDetails);

    box.set(rideId, updated);

    emitDriverPatch(io, driverId, [{ op: "upsert", ride: updated }]);
  }
}

function refreshUserDetailsForUserId(io, userId, userDetails) {
  if (!userId || !userDetails) return;

  for (const [driverId, box] of driverRideInbox.entries()) {
    const ops = [];
    for (const [rideId, ride] of box.entries()) {
      if (ride?.user_id !== userId) continue;

      const updated = attachCustomerFields({
        ...ride,
        user_id: userDetails.user_id ?? ride.user_id ?? null,
        user_name: userDetails.user_name ?? ride.user_name ?? null,
        user_gender: userDetails.user_gender ?? ride.user_gender ?? null,
        user_image: userDetails.user_image ?? ride.user_image ?? null,

        user_phone: userDetails.user_phone ?? ride.user_phone ?? null,
        user_country_code: userDetails.user_country_code ?? ride.user_country_code ?? null,
        user_phone_full: userDetails.user_phone_full ?? ride.user_phone_full ?? null,

        token: userDetails.user_token ?? userDetails.token ?? ride.token ?? null,
        user_details: {
          ...(ride.user_details && typeof ride.user_details === "object" ? ride.user_details : {}),
          ...userDetails,
          user_token: userDetails.user_token ?? userDetails.token ?? null,
          token: userDetails.user_token ?? userDetails.token ?? null,
        },

        _ts: Date.now(),
      }, userDetails);
      box.set(rideId, updated);
      ops.push({ op: "upsert", ride: updated });
    }
    if (ops.length > 0) emitDriverPatch(io, driverId, ops);
  }
}

function findRideInInboxes(rideId) {
  for (const [, box] of driverRideInbox.entries()) {
    const ride = box.get(rideId);
    if (ride) return ride;
  }
  return null;
}

function getRideSnapshotForRedispatch(rideId) {
  return getRideDetails(rideId) || findRideInInboxes(rideId);
}

// ✅ remove ride from all drivers inboxes (safe global scan) + PATCH remove
function removeRideFromAllInboxes(io, rideId) {
  cancelRideTimeout(rideId);
  clearUserRideByRideId(rideId);
  rideDetailsMap.delete(rideId);
  acceptLocks.delete(rideId);

  for (const [driverId, box] of driverRideInbox.entries()) {
    if (box.has(rideId)) {
      box.delete(rideId);
      clearDriverBidStatus(driverId, rideId);

      if (box.size === 0) driverRideInbox.delete(driverId);

      emitDriverPatch(io, driverId, [{ op: "remove", ride_id: rideId }]);
    }
  }

  rideCandidates.delete(rideId);
}

// ✅ Close bidding for a ride (remove from all inboxes) + PATCH remove
function closeRideBidding(io, rideId, opts = {}) {
  cancelRideTimeout(rideId);

  const clearUser = opts.clearUser !== false;
  if (clearUser) clearUserRideByRideId(rideId);
  rideDetailsMap.delete(rideId);
  acceptLocks.delete(rideId);

  for (const [driverId, box] of driverRideInbox.entries()) {
    if (box.has(rideId)) {
      box.delete(rideId);
      clearDriverBidStatus(driverId, rideId);
      if (box.size === 0) driverRideInbox.delete(driverId);

      emitDriverPatch(io, driverId, [{ op: "remove", ride_id: rideId }]);
    }
  }

  rideCandidates.delete(rideId);
}

// ─────────────────────────────
// Dispatch
// ─────────────────────────────
function dispatchToNearbyDrivers(io, data) {
  const rideId = toNumber(data?.ride_id ?? data?.id);
  if (!rideId) return false;
    // ✅ fallback: if dispatch payload missing user info, try snapshot memory by rideId
  const snap0 = getRideDetails(rideId);
  if (snap0 && typeof snap0 === "object") {
    data = {
      ...snap0,   // has user_id/token if previously saved
      ...data,    // incoming overrides snapshot
      user_details: data?.user_details ?? snap0?.user_details ?? null,
    };
  }

  const activeDriverId = getActiveDriverByRide(rideId);
  if (activeDriverId) {
    console.log(`🛑 Dispatch blocked: ride ${rideId} already accepted by driver ${activeDriverId}`);
    return false;
  }

  if (cancelledRides.has(rideId)) {
    console.log(`🛑 Dispatch blocked: ride ${rideId} is cancelled`);
    return false;
  }

  const rawRadius = toNumber(data?.radius) ?? DEFAULT_RADIUS_METERS;
  const radius = Math.max(
    MIN_DISPATCH_RADIUS_METERS,
    Math.min(rawRadius, MAX_DISPATCH_RADIUS_METERS)
  );
  if (rawRadius !== radius) {
    console.log(`[dispatch] radius clamped for ride ${rideId}: ${rawRadius} -> ${radius}`);
  }

  const lat = toNumber(data?.pickup_lat);
  const long = toNumber(data?.pickup_long);

  const serviceTypeId = toNumber(data?.service_type_id) ?? null;
  const base = toNumber(data?.user_bid_price);
  const min = toNumber(data?.min_fare_amount);

  // ✅ build/merge user details from payload + store + token (retry-safe)
  const built = buildUserDetails(data);
  const tokenTmp = getTokenFromAny(data, built, data?.user_details ?? null);

  const userIdTmp = toNumber(
    data?.user_id ?? data?.customer_id ?? data?.passenger_id ?? built?.user_id
  );
  const fromStore = userIdTmp ? getUserDetails(userIdTmp) : null;
  const fromToken = !fromStore && tokenTmp ? getUserDetailsByToken(tokenTmp) : null;

let userDetails = built ?? fromStore ?? fromToken ?? null;

// ✅ extra fallback: if still null, try snapshot user_details
if (!userDetails) {
  const snap = getRideDetails(rideId);
  const snapToken = snap?.token ?? snap?.user_details?.user_token ?? snap?.user_details?.token ?? null;
  const snapUserId = toNumber(snap?.user_id ?? snap?.user_details?.user_id ?? null);

  userDetails =
    (snapUserId ? getUserDetails(snapUserId) : null) ??
    (snapToken ? getUserDetailsByToken(snapToken) : null) ??
    snap?.user_details ??
    null;
}
  const userId = getUserIdFromDispatch(data, userDetails);

  const isPriceUpdated = !!data?.isPriceUpdated;
  const updatedPrice = toNumber(data?.updatedPrice ?? null);
  const updatedAt = toNumber(data?.updatedAt ?? null);

  const storedUser = userId ? getUserDetails(userId) : null;
  const storedByToken = !storedUser && tokenTmp ? getUserDetailsByToken(tokenTmp) : null;

  const routeKm = toNumber(
    data?.route ??
      data?.route_km ??
      data?.routeKm ??
      data?.meta?.route ??
      storedUser?.route ??
      storedByToken?.route ??
      null
  );
  const etaMin = toNumber(
    data?.eta_min ??
      data?.etaMin ??
      data?.meta?.eta_min ??
      storedUser?.eta_min ??
      storedByToken?.eta_min ??
      null
  );


  // ✅ optional filter requirements (apply only if provided)
  const requiredGender = toNumber(
    data?.required_driver_gender ?? data?.required_gender ?? data?.driver_gender ?? null
  );
  const needChildSeat = toNumber(
    data?.need_child_seat ??
      data?.child_seat ??
      data?.require_child_seat ??
      data?.smoking ??
      null
  );
  const needHandicap = toNumber(
    data?.need_handicap ?? data?.handicap ?? data?.require_handicap ?? null
  );

  if (lat === null || long === null) {
    console.log("[dispatch] Invalid dispatch data:", {
      ride_id: rideId,
      pickup_lat: data?.pickup_lat,
      pickup_long: data?.pickup_long,
    });
    return false;
  }

  const nearby = driverLocationService.getNearbyDriversFromMemory(lat, long, radius, {
    only_online: true,
    service_type_id: serviceTypeId,
    max_age_ms: MAX_DRIVER_LOCATION_AGE_MS,

    required_gender: requiredGender,
    need_child_seat: needChildSeat,
    need_handicap: needHandicap,
  });

  const available = nearby.filter((d) => {
    const activeRide = getActiveRideByDriver(toNumber(d.driver_id));
    return !activeRide;
  });

  const candidates =
    MAX_DISPATCH_CANDIDATES > 0 ? available.slice(0, MAX_DISPATCH_CANDIDATES) : available;

  console.log("[dispatch][dispatchToNearbyDrivers]", {
    ride_id: rideId,
    radius,
    service_type_id: serviceTypeId ?? null,
    nearby: nearby.length,
    available: available.length,
    candidates: candidates.length,
    required_gender: requiredGender ?? null,
    need_child_seat: needChildSeat ?? null,
    need_handicap: needHandicap ?? null,
    has_user_details: !!userDetails,
    token_present: !!tokenTmp,
  });

  // ✅ TIMER (SERVER ONLY): ALWAYS CREATE 90s TIMER (NO incoming server_time/expires_at)
  const timer = makeTimer(RIDE_TIMEOUT_S);

  // ✅ start server timeout (seconds)
  startRideTimeout(io, rideId, RIDE_TIMEOUT_S);

  rideCandidates.set(rideId, new Set(candidates.map((d) => d.driver_id)));
  if (userId) setUserActiveRide(userId, rideId);

  const baseMeta =
    data?.meta && typeof data.meta === "object" && !Array.isArray(data.meta) ? data.meta : {};

  const ridePayloadBase = {
    ride_id: rideId,

    // ✅ FLAT user fields (important for retry + merges)
    user_id: userId ?? userDetails?.user_id ?? null,
    user_name: userDetails?.user_name ?? null,
    user_gender: userDetails?.user_gender ?? null,
    user_image: userDetails?.user_image ?? null,
    user_phone: userDetails?.user_phone ?? null,
    user_country_code: userDetails?.user_country_code ?? null,
    user_phone_full: userDetails?.user_phone_full ?? null,

    // ✅ keep token for later accept/merge paths
    token: tokenTmp ?? null,

    pickup_lat: lat,
    pickup_long: long,
    pickup_address: data.pickup_address ?? null,

    destination_lat: toNumber(data.destination_lat),
    destination_long: toNumber(data.destination_long),
    destination_address: data.destination_address ?? null,

    radius,
    user_bid_price: base,
    min_fare_amount: min,

    service_type_id: toNumber(data.service_type_id) ?? null,
    service_category_id: toNumber(data.service_category_id) ?? null,
    created_at: data.created_at ?? null,
    ...(routeKm !== null ? { route: routeKm } : {}),
    ...(etaMin !== null ? { eta_min: etaMin } : {}),
    meta: {
      ...baseMeta,
      ...(routeKm !== null ? { route: routeKm } : {}),
      ...(etaMin !== null ? { eta_min: etaMin } : {}),
    },

    // ✅ full object too
    user_details: userDetails
      ? {
          ...userDetails,
          user_token: userDetails?.user_token ?? userDetails?.token ?? tokenTmp ?? null,
          token: userDetails?.user_token ?? userDetails?.token ?? tokenTmp ?? null,
        }
      : null,

    ...(isPriceUpdated ? { isPriceUpdated: true } : {}),
    ...(updatedPrice !== null ? { updatedPrice } : {}),
    ...(updatedAt !== null ? { updatedAt } : {}),

    // ✅ timer fields (SECONDS)
    ...timer,
  };
  const ridePayload = attachCustomerFields(ridePayloadBase, ridePayloadBase.user_details ?? userDetails);

  // ✅ keep a snapshot for future re-dispatch (retry-safe + includes flat user fields + token)
  saveRideDetails(rideId, attachCustomerFields({
    ride_id: rideId,

    pickup_lat: lat,
    pickup_long: long,
    pickup_address: data.pickup_address ?? null,

    destination_lat: toNumber(data.destination_lat),
    destination_long: toNumber(data.destination_long),
    destination_address: data.destination_address ?? null,

    radius,
    user_bid_price: base,
    min_fare_amount: min,
    service_type_id: serviceTypeId,
    service_category_id: toNumber(data.service_category_id) ?? null,
    created_at: data.created_at ?? null,

    ...(routeKm !== null ? { route: routeKm } : {}),
    ...(etaMin !== null ? { eta_min: etaMin } : {}),
    meta: {
      ...baseMeta,
      ...(routeKm !== null ? { route: routeKm } : {}),
      ...(etaMin !== null ? { eta_min: etaMin } : {}),
    },

    // ✅ user flat fields
    user_id: userId ?? userDetails?.user_id ?? null,
    user_name: userDetails?.user_name ?? null,
    user_gender: userDetails?.user_gender ?? null,
    user_image: userDetails?.user_image ?? null,
    user_phone: userDetails?.user_phone ?? null,
    user_country_code: userDetails?.user_country_code ?? null,
    user_phone_full: userDetails?.user_phone_full ?? null,

    token: tokenTmp ?? null,
    user_details: userDetails
      ? {
          ...userDetails,
          user_token: userDetails?.user_token ?? userDetails?.token ?? tokenTmp ?? null,
          token: userDetails?.user_token ?? userDetails?.token ?? tokenTmp ?? null,
        }
      : null,

    ...(isPriceUpdated ? { isPriceUpdated: true } : {}),
    ...(updatedPrice !== null ? { updatedPrice } : {}),
    ...(updatedAt !== null ? { updatedAt } : {}),

    // ✅ timer fields (SECONDS)
    ...timer,
  }, userDetails));

  // Keep bidRequest user fields stable even when one source is partially missing.
  const bidReqUserId =
    toNumber(ridePayload?.user_id) ??
    toNumber(data?.user_id) ??
    toNumber(userDetails?.user_id) ??
    null;
  const bidReqStoredUser = bidReqUserId ? getUserDetails(bidReqUserId) : null;
  const bidReqStoredByToken =
    !bidReqStoredUser && tokenTmp ? getUserDetailsByToken(tokenTmp) : null;

  const bidReqUserName =
    ridePayload?.user_name ??
    data?.user_name ??
    userDetails?.user_name ??
    bidReqStoredUser?.user_name ??
    bidReqStoredByToken?.user_name ??
    null;
  const bidReqUserGender =
    ridePayload?.user_gender ??
    data?.user_gender ??
    userDetails?.user_gender ??
    bidReqStoredUser?.user_gender ??
    bidReqStoredByToken?.user_gender ??
    null;
  const bidReqUserImage =
    ridePayload?.user_image ??
    data?.user_image ??
    userDetails?.user_image ??
    bidReqStoredUser?.user_image ??
    bidReqStoredByToken?.user_image ??
    null;
  const bidReqUserPhone =
    ridePayload?.user_phone ??
    data?.user_phone ??
    userDetails?.user_phone ??
    bidReqStoredUser?.user_phone ??
    bidReqStoredByToken?.user_phone ??
    null;
  const bidReqUserCountryCode =
    ridePayload?.user_country_code ??
    data?.user_country_code ??
    userDetails?.user_country_code ??
    bidReqStoredUser?.user_country_code ??
    bidReqStoredByToken?.user_country_code ??
    null;
  const bidReqUserPhoneFull =
    ridePayload?.user_phone_full ??
    data?.user_phone_full ??
    userDetails?.user_phone_full ??
    bidReqStoredUser?.user_phone_full ??
    bidReqStoredByToken?.user_phone_full ??
    (bidReqUserCountryCode && bidReqUserPhone
      ? `${bidReqUserCountryCode}${bidReqUserPhone}`
      : null);

  candidates.forEach((d) => {
    const room = driverRoom(d.driver_id);

    io.to(room).emit("ride:bidRequest", sanitizeRidePayloadForClient({
      ride_id: rideId,

      pickup_lat: lat,
      pickup_long: long,
      pickup_address: data.pickup_address ?? null,

      destination_lat: toNumber(data.destination_lat),
      destination_long: toNumber(data.destination_long),
      destination_address: data.destination_address ?? null,

      radius,
      user_bid_price: base,
      min_fare_amount: min,

      user_id: bidReqUserId,
      user_name: bidReqUserName,
      user_gender: bidReqUserGender,
      user_image: bidReqUserImage,
      user_phone: bidReqUserPhone,
      user_country_code: bidReqUserCountryCode,
      user_phone_full: bidReqUserPhoneFull,

      token: tokenTmp ?? null,

      ...(isPriceUpdated ? { isPriceUpdated: true } : {}),
      ...(updatedPrice !== null ? { updatedPrice } : {}),
      ...(updatedAt !== null ? { updatedAt } : {}),

      // ✅ timer fields (SECONDS)
      ...timer,
    }));

    inboxUpsert(d.driver_id, rideId, ridePayload);

    emitDriverPatch(io, d.driver_id, [{ op: "upsert", ride: ridePayload }]);

    console.log(`   -> Sent bidRequest + patch(upsert) to driver ${d.driver_id} (room:${room})`);
  });

  if (candidates.length > 0) {
    console.log(`✅ Finished dispatching ride ${rideId} — notified ${candidates.length} driver(s)`);
  }

  return true;
}

const isCandidateDriver = (rideId, driverId) => {
  const set = rideCandidates.get(rideId);
  if (!set) {
    const inInbox = driverRideInbox.get(driverId)?.has(rideId);
    if (!inInbox) {
      console.log(`🚫 Driver ${driverId} tried to bid on ride ${rideId} but ride is not tracked`);
      return false;
    }
    return true;
  }
  const ok = set.has(driverId);
  if (!ok) {
    console.log(`🚫 Driver ${driverId} tried to bid on ride ${rideId} but is not in candidates`);
  }
  return ok;
};

// ─────────────────────────────
// Socket handler
// ─────────────────────────────
module.exports = (io, socket) => {
  socket.on("driver:getRidesList", ({ driver_id }) => {
    debugLog("driver:getRidesList", { driver_id }, socket.id);
    const driverId = toNumber(driver_id) ?? toNumber(socket.driverId);
    if (!driverId) return;

    emitDriverInbox(io, driverId, "driver:rides:list");
  });

  socket.on("user:joinRideRoom", ({ user_id, ride_id }) => {
    debugLog("user:joinRideRoom", { user_id, ride_id }, socket.id);
    const rideId = toNumber(ride_id);
    if (!rideId) {
      console.log(`undefined ride`);
      return;
    }
    socket.isUser = true;
    socket.userId = toNumber(user_id) ?? null;

    socket.join(rideRoom(rideId));
    socket.currentRideId = rideId;

    if (socket.userId) {
      setUserActiveRide(socket.userId, rideId);
    }
    socket.emit("ride:joined", { ride_id: rideId });

    console.log(
      `👤 User ${socket.userId || "unknown"} joined ride room ${rideRoom(rideId)} (socket:${socket.id})`
    );

    const details = socket.userId ? getUserDetails(socket.userId) : null;
    if (details) {
      updateRideUserDetailsInInbox(io, rideId, details);

      const snapshot = getRideDetails(rideId);
      if (snapshot) {
        const route = toNumber(details?.route ?? null);
        const etaMin = toNumber(details?.eta_min ?? null);
        if (route !== null || etaMin !== null) {
          saveRideDetails(rideId, {
            ...snapshot,
            ...(route !== null ? { route } : {}),
            ...(etaMin !== null ? { eta_min: etaMin } : {}),
            meta: {
              ...(snapshot.meta ?? {}),
              ...(route !== null ? { route } : {}),
              ...(etaMin !== null ? { eta_min: etaMin } : {}),
            },
          });
        }
      }
    }
  });

  socket.on("ride:dispatchToNearbyDrivers", (data) => {
    debugLog("ride:dispatchToNearbyDrivers", data, socket.id);
    console.log("-> Received internal dispatch event:", data);
    dispatchToNearbyDrivers(io, data);
  });

  socket.on("ride:cancel", (payload = {}) => {
    const { ride_id, user_id, reason } = payload;
    debugLog("ride:cancel", { ride_id, user_id, reason }, socket.id);
    const rideId = toNumber(ride_id);
    if (!rideId) return;

    cancelledRides.add(rideId);

    setTimeout(() => {
      cancelledRides.delete(rideId);
    }, CANCELLED_RIDE_TTL_MS);

    removeRideFromAllInboxes(io, rideId);
    rideCandidates.delete(rideId);
    clearActiveRideByRideId(rideId);

    io.to(rideRoom(rideId)).emit("ride:cancelled", {
      ride_id: rideId,
      user_id: toNumber(user_id) ?? null,
      reason: reason ?? null,
      at: Date.now(),
    });

    console.log(`🧨 Ride ${rideId} CANCELLED by user ${user_id ?? "unknown"} | reason=${reason ?? "-"}`);

    const storedUser = user_id ? getUserDetails(toNumber(user_id)) : null;
    const accessToken =
      payload?.access_token ??
      payload?.token ??
      storedUser?.user_token ??
      storedUser?.token ??
      storedUser?.access_token ??
      null;

    const cancelPayload = {
      ride_id: rideId,
      user_id: user_id,
      reason_id: reason,
      sub_ride_id: 0,
      access_token: accessToken || "user-access-token",
    };

    axios
      .post(`${LARAVEL_BASE_URL}/api/customer/transport/cancel-ride`, cancelPayload, {
        timeout: 7000,
      })
      .then((response) => {
        console.log("API Response: Ride Cancelled", response.data);
      })
      .catch((error) => {
        console.error("Error while calling cancel ride API:", error?.response?.data || error.message);
      });
  });

  // ✅ إذا السائق قبل العرض
  socket.on("driver:acceptOffer", (payload) => {
    console.log("[accept][driver:acceptOffer] incoming", {
      ride_id: payload?.ride_id ?? null,
      driver_id: payload?.driver_id ?? socket.driverId ?? null,
      offered_price: payload?.offered_price ?? null,
    });

    const driverId = toNumber(socket.driverId) ?? toNumber(payload?.driver_id);
    const rideId = toNumber(payload?.ride_id);
    const offeredPrice = toNumber(payload?.offered_price);

    if (!driverId || !rideId || offeredPrice === null) return;

    const activeRide = getActiveRideByDriver(driverId);
    if (activeRide && activeRide !== rideId) {
      console.log(
        `⚠️ driver:acceptOffer ignored: driver ${driverId} already active on ride ${activeRide}`
      );
      return;
    }

    const rideSnapshot = driverRideInbox.get(driverId)?.get(rideId) ?? getRideDetails(rideId) ?? null;

    const removed = inboxRemove(driverId, rideId);
    if (removed) {
      clearDriverBidStatus(driverId, rideId);

      const set = rideCandidates.get(rideId);
      if (set) {
        set.delete(driverId);
        if (set.size === 0) rideCandidates.delete(rideId);
      }

      emitDriverPatch(io, driverId, [{ op: "remove", ride_id: rideId }]);

      console.log(`🧹 Removed ride ${rideId} from driver ${driverId} inbox after acceptOffer`);
    }

    const rideDetails = getRideDetails(rideId);
    const userId =
      toNumber(payload?.user_id) ??
      toNumber(rideSnapshot?.user_id) ??
      toNumber(rideDetails?.user_id) ??
      toNumber(rideOwnerByRide.get(rideId));

    // ✅ accept token fallback (retry-safe)
    const tokenTmp =
      payload?.access_token ??
      payload?.token ??
      rideSnapshot?.token ??
      rideSnapshot?.user_details?.user_token ??
      rideSnapshot?.user_details?.token ??
      rideDetails?.token ??
      rideDetails?.user_details?.user_token ??
      rideDetails?.user_details?.token ??
      null;

    const accessToken = tokenTmp;

    io.to(driverRoom(driverId)).emit("ride:driverAccepted", {
      ride_id: rideId,
      driver_id: driverId,
      offered_price: offeredPrice,
      message: "Offer accepted by driver",
      ride_details: rideSnapshot,
      at: Date.now(),
    });

    io.to(rideRoom(rideId)).emit("ride:acceptedByDriver", {
      ride_id: rideId,
      driver_id: driverId,
      offered_price: offeredPrice,
      message: "Offer accepted by driver",
      at: Date.now(),
    });

    const finalize = () =>
      finalizeAcceptedRide(io, rideId, driverId, offeredPrice, {
        message: "Driver accepted the offer",
        rideDetails: rideSnapshot,
        userId,
      });

    if (!userId || !accessToken) {
      console.log(`WARN driver:acceptOffer API skipped: missing user_id/access_token (ride ${rideId})`);
      finalize();
    } else {
      const acceptPayload = {
        user_id: userId,
        access_token: accessToken,
        driver_id: driverId,
        ride_id: rideId,
        offered_price: offeredPrice,
      };

      axios
        .post(`${LARAVEL_BASE_URL}${LARAVEL_ACCEPT_BID_PATH}`, acceptPayload, {
          timeout: LARAVEL_ACCEPT_BID_TIMEOUT_MS,
        })
        .then((response) => {
          console.log("API Response: Accept Bid (driver)", response.data);
          finalize();
        })
        .catch((error) => {
          console.error(
            "Error while calling accept bid API (driver):",
            error?.response?.data || error.message
          );
          // حتى لو فشل API — خلّي الflow يكمل محلياً
          finalize();
        });
    }

    console.log(`✅ Driver ${driverId} ACCEPTED offer ${offeredPrice} for ride ${rideId}`);
  });

  socket.on("driver:submitBid", async (payload) => {
    debugLog("driver:submitBid", payload, socket.id);
    console.log("[bid][driver:submitBid] incoming", {
      ride_id: payload?.ride_id ?? null,
      driver_id: socket.driverId ?? payload?.driver_id ?? null,
      offered_price: payload?.offered_price ?? null,
    });

    const rideId = toNumber(payload?.ride_id);
    const driverId = toNumber(socket.driverId);
    const offeredPrice = toNumber(payload?.offered_price);

    if (!driverId || !rideId || offeredPrice === null) {
      console.log(
        `⚠️ Invalid bid attempt - missing ride_id or driver_id or offered_price (socket: ${socket.id})`
      );
      return;
    }

    const activeRide = getActiveRideByDriver(driverId);
    if (activeRide && activeRide !== rideId) {
      console.log(`⚠️ Driver ${driverId} already active on ride ${activeRide}; bid blocked for ride ${rideId}`);
      return;
    }

    const currency = toNumber(payload?.currency) ?? 1;
    const finalPrice = round2(offeredPrice * currency);
    const rideSnapshot =
      driverRideInbox.get(driverId)?.get(rideId) ??
      getRideDetails(rideId) ??
      null;

    const payloadMeta =
      payload?.meta && typeof payload.meta === "object" && !Array.isArray(payload.meta)
        ? payload.meta
        : {};
    const snapshotMeta =
      rideSnapshot?.meta && typeof rideSnapshot.meta === "object" && !Array.isArray(rideSnapshot.meta)
        ? rideSnapshot.meta
        : {};

    const payloadDriverToPickupDistanceM = toNumber(
      payload?.driver_to_pickup_distance_m ?? payloadMeta?.driver_to_pickup_distance_m ?? null
    );
    const payloadDriverToPickupDurationS = toNumber(
      payload?.driver_to_pickup_duration_s ?? payloadMeta?.driver_to_pickup_duration_s ?? null
    );
    const payloadDriverToPickupDurationMin = toNumber(
      payload?.driver_to_pickup_duration_min ?? payloadMeta?.driver_to_pickup_duration_min ?? null
    );

    const snapshotDriverToPickupDistanceM = toNumber(
      rideSnapshot?.driver_to_pickup_distance_m ?? snapshotMeta?.driver_to_pickup_distance_m ?? null
    );
    const snapshotDriverToPickupDurationS = toNumber(
      rideSnapshot?.driver_to_pickup_duration_s ?? snapshotMeta?.driver_to_pickup_duration_s ?? null
    );
    const snapshotDriverToPickupDurationMin = toNumber(
      rideSnapshot?.driver_to_pickup_duration_min ?? snapshotMeta?.driver_to_pickup_duration_min ?? null
    );

    const driverPos = driverLocationService.getDriver(driverId) || null;
    const driverLat = toNumber(payload?.driver_lat ?? payload?.lat ?? driverPos?.lat ?? null);
    const driverLong = toNumber(payload?.driver_long ?? payload?.long ?? driverPos?.long ?? null);
    const pickupLatForEta = toNumber(payload?.pickup_lat ?? rideSnapshot?.pickup_lat ?? null);
    const pickupLongForEta = toNumber(payload?.pickup_long ?? rideSnapshot?.pickup_long ?? null);

    const driverToPickupDistanceM =
      payloadDriverToPickupDistanceM ??
      snapshotDriverToPickupDistanceM ??
      (driverLat !== null &&
      driverLong !== null &&
      pickupLatForEta !== null &&
      pickupLongForEta !== null
        ? Math.round(getDistanceMeters(driverLat, driverLong, pickupLatForEta, pickupLongForEta))
        : null);

    const payloadDurationFromMin =
      payloadDriverToPickupDurationMin !== null
        ? Math.round(payloadDriverToPickupDurationMin * 60)
        : null;
    const snapshotDurationFromMin =
      snapshotDriverToPickupDurationMin !== null
        ? Math.round(snapshotDriverToPickupDurationMin * 60)
        : null;

    const driverToPickupDurationS =
      payloadDriverToPickupDurationS ??
      payloadDurationFromMin ??
      snapshotDriverToPickupDurationS ??
      snapshotDurationFromMin ??
      (driverToPickupDistanceM !== null
        ? Math.max(60, Math.round((driverToPickupDistanceM * 3.6) / DRIVER_TO_PICKUP_SPEED_KMPH))
        : null);

    const driverToPickupDistanceKm =
      driverToPickupDistanceM !== null ? round2(driverToPickupDistanceM / 1000) : null;
    const driverToPickupDurationMin =
      driverToPickupDurationS !== null ? round2(driverToPickupDurationS / 60) : null;

    const bidMeta = {
      ...payloadMeta,
      ...(driverToPickupDistanceM !== null
        ? {
            driver_to_pickup_distance_m: driverToPickupDistanceM,
            driver_to_pickup_distance_km: driverToPickupDistanceKm,
          }
        : {}),
      ...(driverToPickupDurationS !== null
        ? {
            driver_to_pickup_duration_s: driverToPickupDurationS,
            driver_to_pickup_duration_min: driverToPickupDurationMin,
            estimated_arrival_min: driverToPickupDurationMin,
          }
        : {}),
    };

    const driverMeta = driverLocationService.getMeta(driverId) || {};
    const driverServiceId =
      toNumber(payload?.driver_service_id) ??
      toNumber(payload?.driver_service) ??
      toNumber(socket.driverServiceId) ??
      toNumber(driverMeta?.driver_service_id);

    const accessToken =
      payload?.access_token ?? socket.driverAccessToken ?? driverMeta?.access_token ?? null;

    let driverDetails =
      payload.driver_details ?? {
        driver_name: driverMeta?.driver_name ?? null,
        vehicle_type: driverMeta?.vehicle_type_name ?? null,
        vehicle_number: driverMeta?.plat_no ?? null,
        rating: driverMeta?.rating ?? null,
        driver_image: driverMeta?.driver_image ?? null,
      };

    if (isDriverDetailsEmpty(driverDetails) && driverServiceId && accessToken) {
      const fetched = await fetchDriverMetaFromApi(driverId, accessToken, driverServiceId);
      if (fetched) {
        driverDetails = {
          driver_name: driverDetails?.driver_name ?? fetched.driver_name ?? null,
          vehicle_type: driverDetails?.vehicle_type ?? fetched.vehicle_type ?? null,
          vehicle_number: driverDetails?.vehicle_number ?? fetched.vehicle_number ?? null,
          rating: driverDetails?.rating ?? fetched.rating ?? null,
          driver_image: driverDetails?.driver_image ?? fetched.driver_image ?? null,
        };
      }
    }

    if (cancelledRides.has(rideId)) {
      console.log(`⚠️ Bid ignored: ride ${rideId} is cancelled`);
      return;
    }

    const activeDriverId = getActiveDriverByRide(rideId);
    if (activeDriverId) {
      console.log(`⚠️ Bid ignored: ride ${rideId} already accepted by driver ${activeDriverId}`);
      return;
    }

    if (!isCandidateDriver(rideId, driverId)) return;

    const lastBid = driverLastBidStatus.get(driverId);
    if (lastBid && lastBid.rideId === rideId && !lastBid.responded) {
      console.log(`⚠️ Driver ${driverId} cannot submit a new bid until user responds to the previous bid.`);
      return;
    }

    driverLastBidStatus.set(driverId, { rideId, responded: false });

    if (!driverServiceId || !accessToken) {
      console.log(`⚠️ driver:submitBid API skipped: missing driver_service_id/access_token (driver ${driverId})`);
    } else {
      const bidPayload = {
        driver_id: driverId,
        access_token: accessToken,
        driver_service_id: driverServiceId,
        ride_id: rideId,
        offered_price: offeredPrice,
      };

      axios
        .post(`${LARAVEL_BASE_URL}${LARAVEL_DRIVER_BID_PATH}`, bidPayload, { timeout: 7000 })
        .then((response) => {
          console.log("API Response: Driver Bid Offer", response.data);
        })
        .catch((error) => {
          console.error("Error while calling driver bid API:", error?.response?.data || error.message);
        });
    }

    // ✅ TIMER refresh: every new bid resets timer (90s seconds)
    const timer = refreshRideTimer(io, rideId, {
      update_snapshot: true,
      patch_inboxes: true,
    });

    const ridePayload = {
      ride_id: rideId,
      driver_id: driverId,
      offered_price: offeredPrice,
      bidding_time: Date.now(),

      pickup_lat: payload.pickup_lat ?? null,
      pickup_long: payload.pickup_long ?? null,
      pickup_address: payload.pickup_address ?? null,
      destination_lat: toNumber(payload.destination_lat) ?? null,
      destination_long: toNumber(payload.destination_long) ?? null,
      destination_address: payload.destination_address ?? null,

      radius: payload.radius ?? 0,
      user_bid_price: offeredPrice,
      user_bid_price_final: finalPrice,
      min_fare_amount: payload.min_fare_amount ?? 0,
      service_type_id: toNumber(payload.service_type_id) ?? null,
      service_category_id: toNumber(payload.service_category_id) ?? null,
      created_at: payload.created_at ?? null,
      ...(driverToPickupDistanceM !== null
        ? {
            driver_to_pickup_distance_m: driverToPickupDistanceM,
            driver_to_pickup_distance_km: driverToPickupDistanceKm,
          }
        : {}),
      ...(driverToPickupDurationS !== null
        ? {
            driver_to_pickup_duration_s: driverToPickupDurationS,
            driver_to_pickup_duration_min: driverToPickupDurationMin,
            estimated_arrival_min: driverToPickupDurationMin,
          }
        : {}),
      meta: bidMeta,

      driver_details: driverDetails,
      address_list: payload.address_list ?? [],
      user_timeout: payload.user_timeout ?? 60,
      driver_algo: payload.driver_algo ?? "default_algorithm",
      bid_limit: payload.bid_limit ?? 5,
      can_bid_more: payload.can_bid_more ?? true,
      remain_bid: (payload.bid_limit ?? 5) - (payload.user_bid_count ?? 0),

      // ✅ timer fields (SECONDS)
      ...(timer ? timer : {}),
    };

    io.to(rideRoom(rideId)).emit("ride:newBid", ridePayload);

    console.log(`💰 Driver ${driverId} submitted bid ${offeredPrice} for ride ${rideId}`);

    const removed = inboxRemove(driverId, rideId);
    if (removed) {
      emitDriverPatch(io, driverId, [{ op: "remove", ride_id: rideId }]);
      console.log(`🧹 [inbox] removed ride ${rideId} from driver ${driverId} after submitBid`);
    }
  });

  // ✅ باقي الملف (user:respondToDriver / user:acceptOffer / ride:close / disconnect)
  // ما تم تغييره نهائياً — فقط سيستمر باستعمال refreshRideTimer الذي صار 90s بالثواني
  // -------------------------------------------------

  socket.on("user:respondToDriver", (payload) => {
    console.log("[bid][user:respondToDriver] incoming", {
      ride_id: payload?.ride_id ?? null,
      driver_id: payload?.driver_id ?? null,
      price: payload?.price ?? payload?.user_bid_price ?? null,
    });

    const rideId = toNumber(payload?.ride_id);
    if (!rideId) {
      console.log("[bid][user:respondToDriver] missing ride_id");
      return;
    }

    if (cancelledRides.has(rideId)) {
      console.log(`⚠️ user:respondToDriver ignored: ride ${rideId} is cancelled`);
      return;
    }

    const activeDriverId = getActiveDriverByRide(rideId);
    if (activeDriverId) {
      console.log(`⚠️ user:respondToDriver ignored: ride ${rideId} already accepted by driver ${activeDriverId}`);
      return;
    }

    const newPrice = toNumber(payload?.price) ?? toNumber(payload?.user_bid_price);

    const set = rideCandidates.get(rideId);
    const candidateDrivers = set ? Array.from(set.values()) : [];

    if (newPrice === null) {
      console.log("⚠️ user:respondToDriver ignored: missing price");
      return;
    }

    const snapshot = getRideSnapshotForRedispatch(rideId);

    const payloadToken = payload?.token ?? payload?.access_token ?? payload?.user_token ?? null;
    const fallbackUserId =
      toNumber(payload?.user_id) ?? toNumber(snapshot?.user_id) ?? toNumber(getUserIdForRide(rideId));

    const stored = fallbackUserId ? getUserDetails(fallbackUserId) : null;
    const storedByToken =
      !stored && (payloadToken || snapshot?.token)
        ? getUserDetailsByToken(payloadToken ?? snapshot?.token)
        : null;

    const incomingUserDetails = buildUserDetails(payload);

    const mergedUserDetails = buildUserDetails({
      ...payload,
      user_id: fallbackUserId ?? payload?.user_id,
      token: payloadToken ?? snapshot?.token,
      access_token: payloadToken ?? snapshot?.token,
      user_token: payloadToken ?? snapshot?.token,
      user_details: incomingUserDetails ?? stored ?? storedByToken ?? snapshot?.user_details ?? null,
    });

    if (mergedUserDetails) {
      updateRideUserDetailsInInbox(io, rideId, mergedUserDetails);
    }

    let snapshotDetails = mergedUserDetails ?? snapshot?.user_details ?? null;
    if (!snapshotDetails) {
      const uid = snapshot?.user_id ?? null;
      const details = uid ? getUserDetails(uid) : null;
      if (details) snapshotDetails = details;
    }

    const snapshotBase = snapshot
      ? attachCustomerFields({
          ...snapshot,
          user_details: snapshotDetails ?? snapshot.user_details ?? null,
          user_id: snapshot.user_id ?? snapshotDetails?.user_id ?? null,
          user_name: snapshot.user_name ?? snapshotDetails?.user_name ?? null,
          user_gender: snapshot.user_gender ?? snapshotDetails?.user_gender ?? null,
          user_image: snapshot.user_image ?? snapshotDetails?.user_image ?? null,
          user_phone: snapshot.user_phone ?? snapshotDetails?.user_phone ?? null,
          user_country_code: snapshot.user_country_code ?? snapshotDetails?.user_country_code ?? null,
          user_phone_full: snapshot.user_phone_full ?? snapshotDetails?.user_phone_full ?? null,
          token:
            snapshot?.token ??
            snapshotDetails?.user_token ??
            snapshotDetails?.token ??
            payloadToken ??
            null,
        }, snapshotDetails)
      : null;

    // ✅ TIMER refresh: every user response resets timer (90s seconds)
    const timer = refreshRideTimer(io, rideId, {
      update_snapshot: true,
      patch_inboxes: true,
    });

    for (const dId of candidateDrivers) {
      const rideFull = driverRideInbox.get(dId)?.get(rideId);
      const baseRide = rideFull ?? snapshotBase;

      if (baseRide) {
        const mergedUD = incomingUserDetails ?? baseRide.user_details ?? snapshotDetails ?? null;

        const updatedRide = attachCustomerFields({
          ...baseRide,
          user_details: mergedUD ?? baseRide.user_details ?? null,
          user_id: mergedUD?.user_id ?? baseRide.user_id ?? null,
          user_name: mergedUD?.user_name ?? baseRide.user_name ?? null,
          user_gender: mergedUD?.user_gender ?? baseRide.user_gender ?? null,
          user_image: mergedUD?.user_image ?? baseRide.user_image ?? null,
          user_phone: mergedUD?.user_phone ?? baseRide.user_phone ?? null,
          user_country_code: mergedUD?.user_country_code ?? baseRide.user_country_code ?? null,
          user_phone_full: mergedUD?.user_phone_full ?? baseRide.user_phone_full ?? null,
          token:
            baseRide?.token ??
            mergedUD?.user_token ??
            mergedUD?.token ??
            payloadToken ??
            null,
          user_bid_price: newPrice,
          isPriceUpdated: true,
          updatedPrice: newPrice,
          updatedAt: Date.now(),

          // ✅ timer fields (SECONDS)
          ...(timer ? timer : {}),
        }, mergedUD);

        inboxUpsert(dId, rideId, updatedRide);
        emitDriverPatch(io, dId, [{ op: "upsert", ride: updatedRide }]);
      } else {
        console.log(`ℹ️ ride ${rideId} not found in inbox for driver ${dId} (skip update)`);
      }

      io.to(driverRoom(dId)).emit("ride:userResponse", {
        ride_id: rideId,
        price: newPrice,

        // ✅ timer fields (SECONDS)
        ...(timer ? timer : {}),
        at: Date.now(),
      });

      driverLastBidStatus.set(dId, { rideId, responded: true });
    }

    if (snapshotBase) {
      const redispatchData = {
        ...snapshotBase,
        ride_id: rideId,
        user_bid_price: newPrice,
        isPriceUpdated: true,
        updatedPrice: newPrice,
        updatedAt: Date.now(),

        // ✅ carry same timer forward (SECONDS)
        server_time: timer?.server_time ?? null,
        expires_at: timer?.expires_at ?? null,
        timeout_ms: timer?.timeout_ms ?? null,
      };
      dispatchToNearbyDrivers(io, redispatchData);
    } else {
      console.log(`⚠️ redispatch skipped: no ride snapshot for ride ${rideId}`);
    }

    io.to(rideRoom(rideId)).emit("ride:priceUpdated", {
      ride_id: rideId,
      user_bid_price: newPrice,

      // ✅ timer fields (SECONDS)
      ...(timer ? timer : {}),
      at: Date.now(),
    });

    console.log(
      `✅ user response(broadcast) -> ride ${rideId} newPrice ${newPrice} sent to ${candidateDrivers.length} drivers`
    );
  });

  socket.on("user:acceptOffer", (payload) => {
    console.log("[user:acceptOffer] incoming payload:", payload, "socket:", socket.id);
    debugLog("user:acceptOffer", payload, socket.id);

    const rideId = toNumber(payload?.ride_id);
    const driverId = toNumber(payload?.driver_id);

    if (!rideId || !driverId) {
      console.log("⚠️ user:acceptOffer ignored: missing ride_id/driver_id");
      return;
    }

    const rideSnapshot = getFullRideSnapshot(rideId, driverId);

    if (cancelledRides.has(rideId)) {
      console.log(`⚠️ user:acceptOffer ignored: ride ${rideId} is cancelled`);
      return;
    }

    const activeDriverId = getActiveDriverByRide(rideId);
    if (activeDriverId && activeDriverId !== driverId) {
      console.log(`⚠️ user:acceptOffer ignored: ride ${rideId} already accepted by driver ${activeDriverId}`);
      return;
    }

    let finalPrice = toNumber(payload?.offered_price ?? payload?.price);
    const rideFull = driverRideInbox.get(driverId)?.get(rideId);
    if (finalPrice === null && rideFull) {
      finalPrice = toNumber(
        rideFull.user_bid_price_final ??
          rideFull.updatedPrice ??
          rideFull.user_bid_price ??
          rideFull.min_fare_amount
      );
    }

    if (finalPrice === null) {
      console.log("⚠️ user:acceptOffer ignored: missing offered_price");
      return;
    }

    const rideDetails = getRideDetails(rideId);

    const payloadToken =
      payload?.access_token ?? payload?.token ?? payload?.user_token ?? null;

    const tokenTmp =
      payloadToken ??
      rideSnapshot?.token ??
      rideSnapshot?.user_details?.user_token ??
      rideSnapshot?.user_details?.token ??
      rideDetails?.token ??
      rideDetails?.user_details?.user_token ??
      rideDetails?.user_details?.token ??
      null;

    const userFromToken = tokenTmp ? getUserDetailsByToken(tokenTmp) : null;

    const userId =
      toNumber(payload?.user_id) ??
      toNumber(socket.userId) ??
      toNumber(rideOwnerByRide.get(rideId)) ??
      toNumber(rideDetails?.user_id) ??
      toNumber(userFromToken?.user_id);

    const storedUser = userId ? getUserDetails(userId) : null;
    const accessToken =
      tokenTmp ??
      storedUser?.user_token ??
      storedUser?.token ??
      userFromToken?.user_token ??
      null;

    if (!userId || !accessToken) {
      console.log(`⚠️ user:acceptOffer API skipped: missing user_id/access_token (ride ${rideId})`);
    } else {
      const acceptPayload = {
        user_id: userId,
        access_token: accessToken,
        driver_id: driverId,
        ride_id: rideId,
        offered_price: finalPrice,
      };

      axios
        .post(`${LARAVEL_BASE_URL}${LARAVEL_ACCEPT_BID_PATH}`, acceptPayload, {
          timeout: LARAVEL_ACCEPT_BID_TIMEOUT_MS,
        })
        .then((response) => {
          console.log("API Response: Accept Bid", response.data);
        })
        .catch((error) => {
          console.error("Error while calling accept bid API:", error?.response?.data || error.message);
        });
    }

    finalizeAcceptedRide(io, rideId, driverId, finalPrice, {
      message: "User accepted the offer",
      rideDetails: rideSnapshot,
      userId,
    });

    console.log(`✅ user:acceptOffer -> ride ${rideId} driver ${driverId} price ${finalPrice}`);
  });

  socket.on("ride:close", ({ ride_id }) => {
    debugLog("ride:close", { ride_id }, socket.id);
    const rideId = toNumber(ride_id);
    if (!rideId) return;

    removeRideFromAllInboxes(io, rideId);
    clearActiveRideByRideId(rideId);
    io.to(rideRoom(rideId)).emit("ride:closed", { ride_id: rideId });

    console.log(`🔒 Ride ${rideId} CLOSED`);
  });

  socket.on("disconnect", () => {
    debugLog("disconnect", {}, socket.id);
    const driverId = toNumber(socket.driverId);
    if (driverId) {
      driverRideInbox.delete(driverId);
      driverLastBidStatus.delete(driverId);
      driverPatchSeq.delete(driverId);
    }
  });
};

module.exports.dispatchToNearbyDrivers = dispatchToNearbyDrivers;
module.exports.closeRideBidding = closeRideBidding;
module.exports.refreshUserDetailsForUserId = refreshUserDetailsForUserId;
module.exports.saveRideDetails = saveRideDetails;
module.exports.getRideDetails = getRideDetails;
module.exports.getUserIdForRide = getUserIdForRide;
module.exports.finalizeAcceptedRide = finalizeAcceptedRide;
module.exports.removeRideFromAllInboxes = removeRideFromAllInboxes;
