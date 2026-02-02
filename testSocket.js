const io = require('socket.io-client');
const socket = io('http://localhost:3001');

socket.on('connect', () => {
  console.log('Connected to Socket Server! ID:', socket.id);

  socket.emit('test-ping');

  setTimeout(() => {
    console.log('Sending location update...');
    socket.emit('driver:updateLocation', {
      driver_id: 2,
      lat: 33.5140,
      long: 36.2770
    });

    console.log('Requesting nearby drivers...');
    socket.emit('user:findNearbyDrivers', {
      lat: 22.319022,
      long: 70.7671920
    });
  }, 1500);   
});

socket.on('test-pong', (data) => {
  console.log('Pong received:', data);
});

socket.on('user:nearbyDrivers', (data) => {
  console.log('Nearby Drivers received:', data);
});

socket.on('disconnect', () => {
  console.log('Disconnected');
});