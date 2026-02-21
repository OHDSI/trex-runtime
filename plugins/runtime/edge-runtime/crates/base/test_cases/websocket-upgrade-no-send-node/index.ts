// Use Deno.upgradeWebSocket for proper integration with user worker routing
// This is a "no-send" variant that doesn't send on open, only echoes messages
Deno.serve((req) => {
  const { socket, response } = Deno.upgradeWebSocket(req);

  socket.onmessage = (ev) => {
    socket.send(ev.data);
  };

  return response;
});
