<?php
// docs/ride-status-realtime.php
// Drop-in helper for Laravel controller: emit ONE realtime event per status update.

use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

/**
 * Emit ride status update to Socket server (single realtime event).
 *
 * @param string $socketBase e.g. rtrim(config('services.socket_server_url',''), '/')
 * @param \App\Models\TransportRideBook $ride
 * @param int|null $driverId
 * @param int $status
 * @param \Illuminate\Http\Request $request
 * @param array $extra
 * @return void
 */
private function emitRideStatusEvent($socketBase, $ride, $driverId, $status, $request, array $extra = [])
{
    try {
        Http::timeout(2)->post($socketBase . '/events/internal/ride-status-updated', [
            'ride_id'     => $ride->id,
            'driver_id'   => $driverId,
            'ride_status' => (int) $status,
            'lat'         => $request->get('current_lat') ?? $ride->current_lat ?? null,
            'long'        => $request->get('current_long') ?? $ride->current_long ?? null,
            'payload'     => $extra,
        ]);
    } catch (\Throwable $e) {
        Log::warning('Node ride-status-updated failed', [
            'ride_id' => $ride->id ?? null,
            'error'   => $e->getMessage(),
        ]);
    }
}

/**
 * Example usages inside postDriverUpdateRideStatus:
 *
 * // status 3: arrived
 * $ride->status = 3;
 * $ride->save();
 * $this->emitRideStatusEvent($socketBase, $ride, $driver_id, 3, $request, [
 *     'message' => 'arrived',
 * ]);
 *
 * // status 5: running
 * $old_status = $ride->status;
 * $ride->status = 5;
 * $ride->save();
 * if ($old_status != 5) {
 *     $this->emitRideStatusEvent($socketBase, $ride, $driver_id, 5, $request, [
 *         'message' => 'running',
 *     ]);
 * }
 *
 * // status 4: cancelled
 * $ride->status = 4;
 * $ride->cancel_by = 'driver';
 * $ride->save();
 * $this->emitRideStatusEvent($socketBase, $ride, $driver_id, 4, $request, [
 *     'cancel_by' => $ride->cancel_by,
 * ]);
 *
 * // status 6 or 7 or 9: completed / ended
 * $ride->status = $request_status;
 * $ride->save();
 * $this->emitRideStatusEvent($socketBase, $ride, $driver_id, $request_status, $request, [
 *     'message' => 'completed',
 * ]);
 */
