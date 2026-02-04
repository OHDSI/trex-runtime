// Use Deno.upgradeWebSocket for proper integration with user worker routing
Deno.serve((req) => {
  const { socket, response } = Deno.upgradeWebSocket(req);

  socket.onopen = () => {
    socket.send("meow");
  };

  socket.onmessage = (ev) => {
    socket.send(ev.data);
  };

  return response;
});
