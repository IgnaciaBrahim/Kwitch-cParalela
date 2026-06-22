package cpyd.distributed;

import cpyd.loadtest.MetricsCollector;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

/*implementacion del algoritmo de Ricart y Agrawala para exclusion
mutua distribuida. Sirve para que los nodos se pongan de acuerdo sobre
quien puede crear o modificar un canal (seccion critica).

Tres estados posibles: LIBRE, DESEADO, TOMADO.

El algoritmo usa los timestamps de Lamport para decidir quien tiene
prioridad. Si dos nodos quieren entrar al mismo tiempo, el que tiene
el timestamp menor gana. Si empatan, el de menor ID gana.

Cada nodo corre su instancia con un ServerSocket propio para recibir
los mensajes REQUEST/REPLY/RELEASE de los peers.

Los peers que estan FAILED se ignoran (no se les envia REQUEST ni
se espera su REPLY), para evitar deadlocks cuando un nodo se cae.
*/

public class RicartAgrawala {

    public static class PeerInfo {
        private final String id;
        private final String host;
        private final int port;

        public PeerInfo(String id, String host, int port) {
            this.id = id;
            this.host = host;
            this.port = port;
        }

        public String getId()   { return id; }
        public String getHost() { return host; }
        public int getPort()    { return port; }
    }

    private final String myId;
    private final int myPort;
    private final List<PeerInfo> peers;
    private final NodeMembership membership;
    private final LamportClock clock;
    private final NodeLogger logger;
    private final MetricsCollector metrics;

    private volatile String estado = "LIBRE";
    private volatile int mySeq = 0;
    private final Set<String> repliesReceived = ConcurrentHashMap.newKeySet();
    private final ConcurrentLinkedQueue<MessageWithClock> deferred = new ConcurrentLinkedQueue<>();
    private final Object waitLock = new Object();

    //Ricart-Agrawala coordina la seccion critica entre nodos distintos, pero
    //dentro de un mismo nodo hay muchos hilos (uno por cliente). Este lock hace
    //que solo un hilo local ejecute el protocolo a la vez, asi no se pisan el
    //estado compartido. Es justo (FIFO) para que ninguno se quede esperando para
    //siempre cuando hay mucha carga.
    private final ReentrantLock localCs = new ReentrantLock(true);

    //igual que en HeartbeatMonitor: sin esto, connect() a un peer que se
    //volvio inalcanzable (no solo que cerro el puerto) podria colgar
    //requestCS()/releaseCS() mucho mas que lo que tarda el heartbeat en
    //notar la caida.
    private static final int CONNECT_TIMEOUT_MS = 3000;

    public RicartAgrawala(String myId, int myPort,
                          List<PeerInfo> peers,
                          NodeMembership membership,
                          LamportClock clock,
                          NodeLogger logger,
                          MetricsCollector metrics) {
        this.myId = myId;
        this.myPort = myPort;
        this.peers = peers;
        this.membership = membership;
        this.clock = clock;
        this.logger = logger;
        this.metrics = metrics;

        Thread receiver = new Thread(this::receiverLoop, "RA-receiver-" + myId);
        receiver.setDaemon(true);
        receiver.start();
    }

    public void requestCS() {
        //un solo hilo local a la vez ejecuta el protocolo
        localCs.lock();

        //primero el timestamp y despues el estado, asi un peer que pregunte
        //justo ahora nunca nos ve en DESEADO con un mySeq viejo
        mySeq = clock.tick(); //LC1: timestamp antes de enviar
        estado = "DESEADO";
        repliesReceived.clear();

        List<String> aliveNodes = membership.getAliveNodes();
        int peersNeeded = 0;

        logger.log("[RA] " + myId + " pide CS (seq=" + mySeq + ")");

        //envio REQUEST a cada peer vivo
        for (PeerInfo peer : peers) {
            if (!aliveNodes.contains(peer.getId())) {
                logger.log("[RA] " + peer.getId() + " esta FAILED, se saltea");
                continue;
            }

            try {
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress(peer.getHost(), peer.getPort()), CONNECT_TIMEOUT_MS);
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                ObjectInputStream in = new SafeObjectInputStream(socket.getInputStream());

                MessageWithClock request = new MessageWithClock(mySeq, myId, "REQUEST", null);
                out.writeObject(request);
                out.flush();

                if (metrics != null) metrics.recordCoordinationMessage();

                //espero REPLY o detecto que se difirio
                try {
                    Object resp = in.readObject();
                    if (resp instanceof MessageWithClock rsp
                            && "REPLY".equals(rsp.getType())) {
                        clock.update(rsp.getLamportTime());
                        repliesReceived.add(peer.getId());
                        peersNeeded++;
                        if (metrics != null) metrics.recordCoordinationMessage();
                    }
                } catch (Exception e) {
                    peersNeeded++; //diferido, igual lo cuento
                }

                socket.close();
            } catch (Exception e) {
                logger.log("[RA] No se pudo contactar a " + peer.getId());
            }
        }

        //esperar hasta tener todos los replies
        synchronized (waitLock) {
            while (repliesReceived.size() < peersNeeded) {
                try {
                    waitLock.wait(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                //recalcular por si algun nodo se cayo mientras esperaba
                peersNeeded = 0;
                for (PeerInfo peer : peers) {
                    if (membership.getAliveNodes().contains(peer.getId())) {
                        peersNeeded++;
                    }
                }
            }
        }

        //todos respondieron, entro a CS
        estado = "TOMADO";
        logger.log("[RA] " + myId + " ENTRA a CS (seq=" + mySeq + ")");
    }

    public void releaseCS() {
        try {
            logger.log("[RA] " + myId + " SALE de CS (seq=" + mySeq + ")");
            estado = "LIBRE";

            //envio REPLY a todos los que habian diferido
            MessageWithClock deferredMsg;
            while ((deferredMsg = deferred.poll()) != null) {
                String peerId = deferredMsg.getSenderId();
                for (PeerInfo peer : peers) {
                    if (peer.getId().equals(peerId)) {
                        try {
                            Socket socket = new Socket();
                            socket.connect(new InetSocketAddress(peer.getHost(), peer.getPort()), CONNECT_TIMEOUT_MS);
                            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                            out.flush();

                            int ts = clock.tick();
                            MessageWithClock reply = new MessageWithClock(ts, myId, "REPLY", null);
                            out.writeObject(reply);
                            out.flush();
                            if (metrics != null) metrics.recordCoordinationMessage();

                            socket.close();
                            logger.log("[RA] " + myId + " envio REPLY pendiente a " + peerId);
                        } catch (Exception e) {
                            logger.error("[RA] Error al enviar REPLY diferido a " + peerId);
                        }
                        break;
                    }
                }
            }
        } finally {
            //soltamos el lock local siempre, aunque algo haya fallado al avisar
            if (localCs.isHeldByCurrentThread()) {
                localCs.unlock();
            }
        }
    }

    private void receiverLoop() {
        //escucha en myPort los mensajes RA de otros nodos
        try (ServerSocket serverSocket = new ServerSocket(myPort)) {
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    ObjectInputStream in = new SafeObjectInputStream(socket.getInputStream());
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.flush();

                    Object obj = in.readObject();
                    if (obj instanceof MessageWithClock msg) {
                        clock.update(msg.getLamportTime()); //LC2
                        if (metrics != null) metrics.recordCoordinationMessage();

                        switch (msg.getType()) {
                            case "REQUEST" -> handleRequest(msg, out);
                            case "REPLY"   -> handleReply(msg);
                            case "RELEASE" -> handleRelease(msg);
                            default -> logger.log("[RA] Tipo desconocido: " + msg.getType());
                        }
                    }

                    socket.close();
                } catch (Exception e) {
                    //error normal de conexion
                }
            }
        } catch (Exception e) {
            logger.error("[RA] Error en receiver de " + myId + ": " + e.getMessage());
        }
    }

    //procesa REQUEST: responde REPLY o difiere segun el estado y prioridad
    private void handleRequest(MessageWithClock msg, ObjectOutputStream out) {
        String senderId = msg.getSenderId();
        int senderSeq = msg.getLamportTime();

        logger.log("[RA] " + myId + " recibe REQUEST de " + senderId + " (seq=" + senderSeq + ")");

        try {
            if ("LIBRE".equals(estado)) {
                //estoy libre, respondo REPLY inmediato
                int ts = clock.tick();
                out.writeObject(new MessageWithClock(ts, myId, "REPLY", null));
                out.flush();
                if (metrics != null) metrics.recordCoordinationMessage();
                logger.log("[RA] " + myId + " -> REPLY a " + senderId + " (LIBRE)");

            } else if ("TOMADO".equals(estado)) {
                //estoy en CS, difiero la respuesta
                deferred.add(msg);
                logger.log("[RA] " + myId + " difiere REQUEST de " + senderId + " (TOMADO)");

            } else if ("DESEADO".equals(estado)) {
                //ambos queremos CS, comparar prioridad (timestamp, despues ID)
                boolean iHavePriority;
                if (mySeq < senderSeq) {
                    iHavePriority = true;
                } else if (mySeq > senderSeq) {
                    iHavePriority = false;
                } else {
                    iHavePriority = myId.compareTo(senderId) < 0;
                }

                if (iHavePriority) {
                    deferred.add(msg);
                    String motivo = (mySeq != senderSeq)
                        ? ("menor seq " + mySeq + "<" + senderSeq)
                        : ("empate seq=" + mySeq + ", gana id menor: " + myId + "<" + senderId);
                    logger.log("[RA] " + myId + " difiere REQUEST de " + senderId
                        + " (tengo prioridad: " + motivo + ")");
                } else {
                    int ts = clock.tick();
                    out.writeObject(new MessageWithClock(ts, myId, "REPLY", null));
                    out.flush();
                    if (metrics != null) metrics.recordCoordinationMessage();
                    logger.log("[RA] " + myId + " -> REPLY a " + senderId
                        + " (el tiene prioridad: " + senderSeq + "<" + mySeq + ")");
                }
            }
        } catch (Exception e) {
            logger.error("[RA] Error en handleRequest: " + e.getMessage());
        }
    }

    //cuenta REPLY y despierta a requestCS() si esta esperando
    private void handleReply(MessageWithClock msg) {
        logger.log("[RA] " + myId + " recibe REPLY de " + msg.getSenderId());
        repliesReceived.add(msg.getSenderId());

        synchronized (waitLock) {
            waitLock.notifyAll();
        }
    }

    private void handleRelease(MessageWithClock msg) {
        logger.log("[RA] " + myId + " recibe RELEASE de " + msg.getSenderId());
    }
}
