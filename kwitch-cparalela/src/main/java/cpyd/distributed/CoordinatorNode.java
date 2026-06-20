package cpyd.distributed;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;


/*tercer nodo del sistema (puerto 7000). Sirve como punto de coordinacion
para que StreamServer y ChatServer se conozcan entre si.

Mantiene la lista de miembros, participa en heartbeats para detectar
caidas, y participa en Ricart-Agrawala como nodo votante.

Escribe logs en logs/node_CoordinatorNode.log.
*/

public class CoordinatorNode {

    private static final int PORT = 7000;
    private static final int HB_PORT = 7001;
    private static final int RA_PORT = 7002;

    private final NodeMembership membership = new NodeMembership();
    private final LamportClock clock = new LamportClock();
    private final NodeLogger logger = new NodeLogger("CoordinatorNode");
    private RicartAgrawala ricart;
    private HeartbeatMonitor heartbeatMonitor;

    public static void main(String[] args) {
        new CoordinatorNode().start();
    }

    public void start() {
        membership.registerNode("CoordinatorNode", "localhost", PORT);
        membership.registerNode("StreamServer", "localhost", 5000);
        membership.registerNode("ChatServer", "localhost", 6000);

        logger.log("[CoordinatorNode] Iniciando en puerto " + PORT
            + " (HB:" + HB_PORT + ", RA:" + RA_PORT + ")");

        //iniciar Ricart-Agrawala
        List<RicartAgrawala.PeerInfo> raPeers = List.of(
            new RicartAgrawala.PeerInfo("StreamServer", "localhost", 5002),
            new RicartAgrawala.PeerInfo("ChatServer", "localhost", 6002)
        );
        ricart = new RicartAgrawala("CoordinatorNode", RA_PORT, raPeers, membership, clock, logger, null);

        //iniciar heartbeat
        List<NodeMembership.NodeInfo> hbPeers = List.of(
            new NodeMembership.NodeInfo("StreamServer", "localhost", 5001),
            new NodeMembership.NodeInfo("ChatServer", "localhost", 6001)
        );
        heartbeatMonitor = new HeartbeatMonitor(
            "CoordinatorNode", HB_PORT, hbPeers, membership, clock, logger
        );
        heartbeatMonitor.start();

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            logger.log("[CoordinatorNode] Esperando conexiones...");

            while (true) {
                Socket socket = serverSocket.accept();
                CoordinatorHandler handler = new CoordinatorHandler(socket, this);
                Thread thread = new Thread(handler);
                thread.start();
            }

        } catch (Exception e) {
            logger.error("[CoordinatorNode] Error fatal: " + e.getMessage());
        }
    }

    public NodeMembership getMembership() { return membership; }
    public LamportClock getClock()        { return clock; }
    public NodeLogger getLogger()         { return logger; }
}

class CoordinatorHandler implements Runnable {

    private final Socket socket;
    private final CoordinatorNode node;

    public CoordinatorHandler(Socket socket, CoordinatorNode node) {
        this.socket = socket;
        this.node = node;
    }

    @Override
    public void run() {
        try {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            ObjectInputStream in = new SafeObjectInputStream(socket.getInputStream());

            //leer el mensaje entrante y actuar segun su tipo
            Object obj = in.readObject();

            if (obj instanceof MessageWithClock msg) {

                //LC2: actualizar reloj al recibir
                node.getClock().update(msg.getLamportTime());
                String type = msg.getType();

                if ("REGISTER".equals(type)) {
                    //alguien se registra en el sistema
                    NodeMembership.NodeInfo info = (NodeMembership.NodeInfo) msg.getPayload();
                    //el nodo queda anotado en la lista de miembros del coordinador
                    node.getMembership().registerNode(info.getId(), info.getHost(), info.getPort());

                    node.getLogger().log("[CoordinatorNode] Nodo registrado: " + info.getId()
                        + " (" + info.getHost() + ":" + info.getPort() + ")"
                        + " | membresia=" + node.getMembership().getAliveNodes());
                    out.writeObject(new MessageWithClock(
                        node.getClock().tick(), "CoordinatorNode", "REGISTER_OK", null
                    ));
                    out.flush();

                } else if ("MEMBERSHIP".equals(type)) {
                    //alguien pide la lista de nodos activos
                    List<String> alive = node.getMembership().getAliveNodes();
                    out.writeObject(new MessageWithClock(
                        node.getClock().tick(), "CoordinatorNode", "MEMBERSHIP_OK", (Object) alive
                    ));
                    out.flush();

                } else if ("DIE".equals(type)) {
                    //falla inducida: apagar este nodo
                    node.getLogger().log("[CoordinatorNode] Recibido DIE. Apagando...");
                    out.writeObject(new MessageWithClock(
                        node.getClock().tick(), "CoordinatorNode", "DYING", null
                    ));
                    out.flush();
                    socket.close();
                    System.exit(0);

                } else {
                    node.getLogger().log("[CoordinatorNode] Tipo desconocido: " + type);
                }
            }

            socket.close();

        } catch (Exception e) {
            node.getLogger().error("[CoordinatorNode] Error en handler: " + e.getMessage());
        }
    }
}
