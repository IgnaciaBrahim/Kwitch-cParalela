package cpyd.distributed;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

//objetivo:

/*monitorea si los otros nodos del sistema estan vivos.

Cada nodo tiene un HeartbeatMonitor. Un hilo envia latidos cada 5s a los
peers, otro hilo recibe los latidos de los peers, y otro hilo revisa si
algun peer no ha enviado latido en 15s. si pasa eso, lo marca FAILED.

Escribe logs en el NodeLogger del nodo que lo crea.
*/

public class HeartbeatMonitor {

    private final String myId;
    private final int myHbPort;
    private final List<NodeMembership.NodeInfo> peers;
    private final NodeMembership membership;
    private final LamportClock clock;
    private final NodeLogger logger;

    private final int interval = 5000;
    private final int timeout  = 15000;

    private final ConcurrentHashMap<String, Long> lastHeartbeat = new ConcurrentHashMap<>();
    private volatile boolean running = false;

    public HeartbeatMonitor(String myId, int myHbPort,
                            List<NodeMembership.NodeInfo> peers,
                            NodeMembership membership,
                            LamportClock clock,
                            NodeLogger logger) {
        this.myId = myId;
        this.myHbPort = myHbPort;
        this.peers = peers;
        this.membership = membership;
        this.clock = clock;
        this.logger = logger;

        long now = System.currentTimeMillis();
        for (NodeMembership.NodeInfo peer : peers) {
            lastHeartbeat.put(peer.getId(), now);
        }
    }

    public void start() {
        running = true;

        Thread sender = new Thread(this::senderLoop, "HB-sender-" + myId);
        sender.setDaemon(true);
        sender.start();

        Thread receiver = new Thread(this::receiverLoop, "HB-receiver-" + myId);
        receiver.setDaemon(true);
        receiver.start();

        Thread checker = new Thread(this::checkerLoop, "HB-checker-" + myId);
        checker.setDaemon(true);
        checker.start();

        logger.log("[Heartbeat] Monitor iniciado en puerto " + myHbPort);
    }

    public void stop() {
        running = false;
    }

    private void senderLoop() {
        while (running) {
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                break;
            }

            for (NodeMembership.NodeInfo peer : peers) {
                try {
                    Socket socket = new Socket(peer.getHost(), peer.getPort());
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.flush();
                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                    int ts = clock.tick();
                    MessageWithClock hb = new MessageWithClock(ts, myId, "HEARTBEAT", null);
                    out.writeObject(hb);
                    out.flush();

                    socket.close();
                } catch (Exception e) {
                    //el checker lo va a detectar por timeout
                }
            }
        }
    }

    private void receiverLoop() {
        try (ServerSocket serverSocket = new ServerSocket(myHbPort)) {
            while (running) {
                try {
                    Socket socket = serverSocket.accept();
                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.flush();

                    Object obj = in.readObject();
                    if (obj instanceof MessageWithClock msg
                            && "HEARTBEAT".equals(msg.getType())) {

                        clock.update(msg.getLamportTime());
                        lastHeartbeat.put(msg.getSenderId(), System.currentTimeMillis());
                        membership.markAlive(msg.getSenderId());
                    }

                    socket.close();
                } catch (Exception e) {
                    //error normal de conexion
                }
            }
        } catch (Exception e) {
            logger.error("[Heartbeat] Error en receiver: " + e.getMessage());
        }
    }

    private void checkerLoop() {
        while (running) {
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                break;
            }

            long now = System.currentTimeMillis();
            for (NodeMembership.NodeInfo peer : peers) {
                Long last = lastHeartbeat.get(peer.getId());
                if (last != null && (now - last) > timeout) {
                    membership.markFailed(peer.getId());
                    logger.log("[Heartbeat] Nodo " + peer.getId()
                        + " marcado como FAILED (sin heartbeat por " + (now - last) + "ms)");
                }
            }
        }
    }
}
