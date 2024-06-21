export const upgradeWebSocket = (
  _req: Request,
): { response: Response; socket: WebSocket } => {
  const { 0: client, 1: socket } = new WebSocketPair();
  socket.accept();
  return {
    response: new Response(null, { status: 101, webSocket: client }),
    socket,
  };
};
