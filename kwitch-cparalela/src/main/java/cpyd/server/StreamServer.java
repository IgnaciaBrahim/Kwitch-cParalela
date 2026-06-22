package cpyd.server;

import cpyd.distributed.HeartbeatMonitor;
import cpyd.distributed.LamportClock;
import cpyd.distributed.MessageWithClock;
import cpyd.distributed.NodeLogger;
import cpyd.distributed.NodeMembership;
import cpyd.distributed.RicartAgrawala;
import cpyd.distributed.SafeObjectInputStream;
import cpyd.handler.ClientHandler;
import cpyd.loadtest.MetricsCollector;
import cpyd.model.StreamSession;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    //atendemos a cada cliente en un hilo de este pool. El tamano es fijo a
    //proposito: limita cuantas conexiones se atienden a la vez para que el
    //servidor no se quede sin memoria si llegan muchisimas conexiones de golpe.
    public static final int MAX_CLIENTES = 200;
    private static final ExecutorService clientPool = Executors.newFixedThreadPool(MAX_CLIENTES);

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
        membership.registerNode("ChatServer", "localhost", 6000);
        membership.registerNode("CoordinatorNode", "localhost", 7000);

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

        //re-registrarse cada 30s para reintegracion si CoordinatorNode vuelve
        Thread reReg = new Thread(() -> {
            while (true) {
                try { Thread.sleep(30000); } catch (InterruptedException e) { break; }
                registerWithCoordinator();
            }
        }, "reReg-StreamServer");
        reReg.setDaemon(true);
        reReg.start();

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                logger.log("[StreamServer] Cliente conectado: " + clientSocket.getInetAddress());
                ClientHandler handler = new ClientHandler(clientSocket);
                clientPool.execute(handler);
            }
        } catch (IOException e) {
            logger.error("[StreamServer] Error fatal del servidor: " + e.getMessage());
        }
    }

    private static void registerWithCoordinator() {
        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress("localhost", 7000), 3000);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            ObjectInputStream in = new SafeObjectInputStream(socket.getInputStream());

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
