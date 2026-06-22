package cpyd.server;

import cpyd.distributed.HeartbeatMonitor;
import cpyd.distributed.LamportClock;
import cpyd.distributed.MessageWithClock;
import cpyd.distributed.NodeLogger;
import cpyd.distributed.NodeMembership;
import cpyd.distributed.RicartAgrawala;
import cpyd.distributed.SafeObjectInputStream;
import cpyd.handler.ChatClientHandler;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//objetivo:

/*servidor de chat (puerto 6000). Los viewers se conectan aca para
enviar y recibir mensajes de chat con ordenamiento Lamport.

Ahora tambien tiene heartbeat, membresia, Ricart-Agrawala (votante)
y un logger que escribe a archivo logs/node_ChatServer.log.
*/

public class ChatServer {
    private static final int PORT = 6000;
    private static final int HB_PORT = 6001;
    private static final int RA_PORT = 6002;

    //pool de hilos fijo: cada conexion de chat se atiende en un hilo, pero con
    //un tope para no quedarnos sin memoria si llegan demasiadas a la vez. Las
    //conexiones que sobran esperan en cola en vez de crear hilos sin limite.
    private static final int MAX_CLIENTES = 200;

    private final Map<String, List<ChatClientHandler>> channels = new ConcurrentHashMap<>();
    private final ExecutorService threadPool = Executors.newFixedThreadPool(MAX_CLIENTES);

    private final LamportClock clock = new LamportClock();
    private final NodeMembership membership = new NodeMembership();
    private final NodeLogger logger = new NodeLogger("ChatServer");
    private final String nodeId = "ChatServer";

    public static void main(String[] args) {
        new ChatServer().startServer();
    }

    public void startServer() {
        membership.registerNode(nodeId, "localhost", PORT);
        membership.registerNode("StreamServer", "localhost", 5000);
        membership.registerNode("CoordinatorNode", "localhost", 7000);

        logger.log("[ChatServer] Iniciando en puerto " + PORT
            + " (HB:" + HB_PORT + ", RA:" + RA_PORT + ")");

        //iniciar Ricart-Agrawala (solo para responder REQUESTs)
        List<RicartAgrawala.PeerInfo> raPeers = List.of(
            new RicartAgrawala.PeerInfo("StreamServer", "localhost", 5002),
            new RicartAgrawala.PeerInfo("CoordinatorNode", "localhost", 7002)
        );
        new RicartAgrawala(nodeId, RA_PORT, raPeers, membership, clock, logger, null);

        //iniciar HeartbeatMonitor
        List<NodeMembership.NodeInfo> hbPeers = List.of(
            new NodeMembership.NodeInfo("StreamServer", "localhost", 5001),
            new NodeMembership.NodeInfo("CoordinatorNode", "localhost", 7001)
        );
        HeartbeatMonitor hb = new HeartbeatMonitor(nodeId, HB_PORT, hbPeers, membership, clock, logger);
        hb.start();

        //registrarse con CoordinatorNode
        registerWithCoordinator();

        //re-registrarse cada 30s
        Thread reReg = new Thread(() -> {
            while (true) {
                try { Thread.sleep(30000); } catch (InterruptedException e) { break; }
                registerWithCoordinator();
            }
        }, "reReg-ChatServer");
        reReg.setDaemon(true);
        reReg.start();

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            logger.log("[ChatServer] Escuchando en puerto " + PORT + "...");

            while (true) {
                Socket clientSocket = serverSocket.accept();
                ChatClientHandler handler = new ChatClientHandler(clientSocket, this);
                threadPool.execute(handler);
            }
        } catch (IOException e) {
            logger.error("[ChatServer] Fallo critico: " + e.getMessage());
        } finally {
            threadPool.shutdown();
        }
    }

    private void registerWithCoordinator() {
        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress("localhost", 7000), 3000);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            ObjectInputStream in = new SafeObjectInputStream(socket.getInputStream());

            NodeMembership.NodeInfo info = new NodeMembership.NodeInfo(nodeId, "localhost", HB_PORT);
            int ts = clock.tick();
            MessageWithClock msg = new MessageWithClock(ts, nodeId, "REGISTER", info);
            out.writeObject(msg);
            out.flush();

            in.readObject();
            socket.close();
            logger.log("[ChatServer] Registrado con CoordinatorNode");
        } catch (Exception e) {
            logger.error("[ChatServer] No se pudo registrar: " + e.getMessage());
        }
    }

    public void addClientToChannel(String channelId, ChatClientHandler client) {
        channels.computeIfAbsent(channelId, k -> new CopyOnWriteArrayList<>()).add(client);
    }

    public void removeClientFromChannel(String channelId, ChatClientHandler client) {
        List<ChatClientHandler> channelClients = channels.get(channelId);
        if (channelClients != null) {
            channelClients.remove(client);
            if (channelClients.isEmpty()) {
                channels.remove(channelId);
            }
        }
    }

    public List<ChatClientHandler> getChannelClients(String channelId) {
        return channels.get(channelId);
    }

    public LamportClock getClock() { return clock; }
    public String getNodeId()      { return nodeId; }
    public NodeLogger getLogger()  { return logger; }
}
