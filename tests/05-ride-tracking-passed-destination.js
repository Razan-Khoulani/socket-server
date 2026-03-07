const assert = require("assert");
const path = require("path");

const {
  setActiveRide,
  clearActiveRideByRideId,
} = require("../store/activeRides.store");

const rideTrackingModulePath = path.resolve(__dirname, "../services/rideTracking.js");
const osrmModulePath = path.resolve(__dirname, "../services/osrm.service.js");

function loadRideTrackingWithMockedOsrm(sequence) {
  const osrmModule = require(osrmModulePath);
  let index = 0;
  const fallback = sequence.length > 0 ? sequence[sequence.length - 1] : null;

  osrmModule.getRoadDistanceMeters = async () => {
    const value = index < sequence.length ? sequence[index] : fallback;
    index += 1;
    return value;
  };

  delete require.cache[rideTrackingModulePath];
  return require(rideTrackingModulePath);
}

function createIoCapture() {
  const events = [];
  const io = {
    to(room) {
      return {
        emit(event, payload) {
          events.push({ room, event, payload });
        },
      };
    },
  };
  return { io, events };
}

async function runPassedDestinationFlowTest() {
  process.env.RIDE_TRACK_MIN_INSIDE_MS = "0";
  process.env.RIDE_TRACK_ROAD_CHECK_EVERY_MS = "0";
  process.env.RIDE_TRACK_AIR_TRIGGER_M = "250";
  process.env.RIDE_TRACK_ENTER_AIR_M = "25";
  process.env.RIDE_TRACK_ENTER_ROAD_M = "60";

  const rideTracking = loadRideTrackingWithMockedOsrm([10, 40]);
  const { io, events } = createIoCapture();

  const rideId = 9101;
  const driverId = 5101;

  setActiveRide(driverId, rideId);

  const started = rideTracking.startTracking(
    io,
    rideId,
    { lat: 33.5000, long: 36.3000 },
    { lat: 33.5001, long: 36.3001 }
  );

  assert.strictEqual(started, true, "startTracking should start a new tracking session");

  await rideTracking.updateLocation(io, rideId, 33.5001, 36.3001);
  await rideTracking.updateLocation(io, rideId, 33.5004, 36.3004);

  const driverArrivedEvents = events.filter(
    (e) => e.room === `driver:${driverId}` && e.event === "ride:arrivedDestination"
  );
  const driverPassedEvents = events.filter(
    (e) => e.room === `driver:${driverId}` && e.event === "ride:passedDestination"
  );

  assert.strictEqual(
    driverArrivedEvents.length,
    1,
    "driver should receive one ride:arrivedDestination event"
  );
  assert.strictEqual(
    driverPassedEvents.length,
    1,
    "driver should receive one ride:passedDestination event"
  );

  const passedPayload = driverPassedEvents[0].payload;
  assert.strictEqual(
    passedPayload.trigger,
    "within_25m_destination",
    "passed event trigger should be within_25m_destination"
  );
  assert.strictEqual(
    passedPayload.threshold_m,
    25,
    "threshold_m should be 25"
  );
  assert.strictEqual(
    passedPayload.road_distance_m,
    10,
    "road_distance_m should follow mocked OSRM value"
  );

  clearActiveRideByRideId(rideId);

  return {
    ride_id: rideId,
    driver_id: driverId,
    arrived_events: driverArrivedEvents.length,
    passed_events: driverPassedEvents.length,
    passed_payload: passedPayload,
  };
}

(async () => {
  try {
    const result = await runPassedDestinationFlowTest();
    console.log("PASS: rideTracking passed-destination flow");
    console.log(JSON.stringify(result, null, 2));
  } catch (error) {
    console.error("FAIL: rideTracking passed-destination flow");
    console.error(error?.stack || error?.message || error);
    process.exit(1);
  }
})();
