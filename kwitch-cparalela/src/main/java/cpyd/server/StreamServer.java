package cpyd.server;

import cpyd.distributed.HeartbeatMonitor;
import cpyd.distributed.LamportClock;
import cpyd.distributed.MessageWithClock;
import cpyd.distributed.NodeLogger;
import cpyd.distributed.NodeMembership;
import cpyd.distributed.RicartAgrawala;
import cpyd.handler.ClientHandler;
import cpyd.loadtest.MetricsCollector;
import cpyd.model.StreamSession;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

//objetivo

/*servidor de streams. Atiende conexiones de streamers y viewers en el
puerto 5000.

Ahora tiene reloj de Lamport, miembros de nodos, heartbeat y el
algoritmo Ricart-Agrawala para que solo un nodo pueda crear o modificar
canales a la vez.

Al iniciar se registra con el CoordinatorNode y empieza a mandar
heartbeats para que los demas sepan que esta vivo.
*/

public class StreamServer {

    public static final int PORT = 5000;
    public static final int HB_PORT = 5001;
    public static final int RA_PORT = 5002;

    public static final ConcurrentHashMap<String, StreamSession> activeSessions =
                                                    new ConcurrentHashMap<>();
    public static final ConcurrentHashMap<String, CopyOnWriteArrayList<ClientHandler>> subscribers =
                                                                        new ConcurrentHashMap<>();

    //componentes distribuidos
    public static final LamportClock clock = new LamportClock();
    public static final NodeMembership membership = new NodeMembership();
    public static final NodeLogger logger = new NodeLogger("StreamServer");
    public static final MetricsCollector metrics = new MetricsCollector();
    public static RicartAgrawala ricart;

    public static void main(String[] args) {
        logger.log("[StreamServer] Iniciando en puerto " + PORT
            + " (HB:" + HB_PORT + ", RA:" + RA_PORT + ")");

        membership.registerNode("StreamServer", "localhost", PORT);

        //iniciar Ricart-Agrawala con logger y metricas
        List<RicartAgrawala.PeerInfo> raPeers = List.of(
            new RicartAgrawala.PeerInfo("ChatServer", "localhost", 6002),
            new RicartAgrawala.PeerInfo("CoordinatorNode", "localhost", 7002)
        );
        ricart = new RicartAgrawala("StreamServer", RA_PORT, raPeers, membership, clock, logger, metrics);

        //iniciar HeartbeatMonitor
        List<NodeMembership.NodeInfo> hbPeers = List.of(
            new NodeMembership.NodeInfo("ChatServer", "localhost", 6001),
            new NodeMembership.NodeInfo("CoordinatorNode", "localhost", 7001)
        );
        HeartbeatMonitor hb = new HeartbeatMonitor("StreamServer", HB_PORT, hbPeers, membership, clock, logger);
        hb.start();

        //registrarse con CoordinatorNode
        registerWithCoordinator();

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                logger.log("[StreamServer] Cliente conectado: " + clientSocket.getInetAddress());
                ClientHandler handler = new ClientHandler(clientSocket);
                Thread thread = new Thread(handler);
                thread.start();
            }
        } catch (IOException e) {
            logger.error("[StreamServer] Error fatal del servidor: " + e.getMessage());
        }
    }

    private static void registerWithCoordinator() {
        try {
            Socket socket = new Socket("localhost", 7000);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            NodeMembership.NodeInfo info = new NodeMembership.NodeInfo("StreamServer", "localhost", HB_PORT);
            int ts = clock.tick();
            MessageWithClock msg = new MessageWithClock(ts, "StreamServer", "REGISTER", info);
            out.writeObject(msg);
            out.flush();

            in.readObject();
            socket.close();
            logger.log("[StreamServer] Registrado con CoordinatorNode");
        } catch (Exception e) {
            logger.error("[StreamServer] No se pudo registrar con CoordinatorNode: " + e.getMessage());
        }
    }
}
