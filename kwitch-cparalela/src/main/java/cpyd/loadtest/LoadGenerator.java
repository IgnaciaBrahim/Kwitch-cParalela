package cpyd.loadtest;

import cpyd.distributed.MessageWithClock;
import cpyd.distributed.SafeObjectInputStream;
import cpyd.model.ChatMessage;
import cpyd.model.ServerResponse;
import cpyd.model.StreamSession;
import cpyd.model.StreamStatus;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;


/*generador de carga para la prueba de trafico real.

Lanza 50 hilos concurrentes que actuan como streamers y viewers.
Cada hilo crea un canal (ejercita Ricart-Agrawala), se suscribe a el,
y envia mensajes de chat. Mide la latencia de cada operacion.

A los 30 segundos envia un mensaje DIE al CoordinatorNode para
inducir una falla y medir el tiempo de recuperacion.

Al final imprime un reporte con throughput, latencia promedio y p95,
tasa de error y mensajes de coordinacion estimados.
*/

public class LoadGenerator {

    private static final String HOST = "localhost";
    private static final int STREAM_PORT = 5000;
    private static final int CHAT_PORT = 6000;
    private static final int COORD_PORT = 7000;

    private static final int NUM_THREADS = 50;
    private static final long TEST_DURATION_MS = 65_000; //65 segundos
    private static final long DIE_AFTER_MS = 30_000;     //DIE a los 30s

    private final MetricsCollector metrics = new MetricsCollector();
    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final AtomicInteger activeThreads = new AtomicInteger(0);

    //para medir recuperacion
    private volatile long fallaTime = 0;
    private volatile long recoveryTime = 0;
    private volatile boolean fallaInducida = false;

    public static void main(String[] args) {
        new LoadGenerator().start();
    }

    public void start() {
        System.out.println("========== GENERADOR DE CARGA ==========");
        System.out.println("Hilos: " + NUM_THREADS + " | Duracion: " + (TEST_DURATION_MS/1000) + "s");
        System.out.println("Coordinador: " + HOST + ":" + COORD_PORT);
        System.out.println("DIE programado a los " + (DIE_AFTER_MS/1000) + "s\n");

        ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);

        //lanzar los hilos de carga
        for (int i = 0; i < NUM_THREADS; i++) {
            final int threadId = i;
            pool.execute(() -> runLoad(threadId));
        }

        //esperar a que todos los hilos esten listos
        while (activeThreads.get() < NUM_THREADS) {
            try { Thread.sleep(100); } catch (InterruptedException e) { break; }
        }

        System.out.println("[Carga] Todos los hilos listos. Iniciando en 1 segundo...");
        try { Thread.sleep(1000); } catch (InterruptedException e) { }

        long startTime = System.currentTimeMillis();
        startLatch.countDown(); //todos parten al mismo tiempo

        //hilo que envia DIE al CoordinatorNode a los 30s
        Thread dieThread = new Thread(this::sendDie);
        dieThread.setDaemon(true);
        dieThread.start();

        //esperar a que termine el tiempo de prueba
        try {
            Thread.sleep(TEST_DURATION_MS);
        } catch (InterruptedException e) { }

        pool.shutdownNow();
        System.out.println("\n[Carga] Prueba terminada.");

        //imprimir reporte
        metrics.printReport();

        //imprimir recuperacion si hubo falla
        if (fallaInducida && recoveryTime > 0) {
            long recuperacionMs = recoveryTime - fallaTime;
            System.out.println("Tiempo de recuperacion:   " + recuperacionMs + " ms");
            System.out.println("==========================================\n");
        }

        //le preguntamos al StreamServer el conteo real de mensajes de coordinacion
        long coordReal = queryCoordinationCount();
        if (coordReal >= 0) {
            System.out.println("Msjs coordinacion (real): " + coordReal
                + "  <- conteo exacto del StreamServer (Ricart-Agrawala)");
        } else {
            System.out.println("Msjs coordinacion: no disponible (StreamServer no respondio)");
        }
        System.out.println("(detalle evento por evento en logs/node_StreamServer.log)");
        System.out.println("==========================================\n");
    }

    /*pregunta al StreamServer cuantos mensajes de coordinacion (Ricart-Agrawala)
    genero realmente durante la prueba. Devuelve -1 si no se pudo obtener.*/
    private long queryCoordinationCount() {
        try (Socket socket = new Socket(HOST, STREAM_PORT)) {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            ObjectInputStream in = new SafeObjectInputStream(socket.getInputStream());
            out.writeObject("GET_METRICS");
            out.flush();
            Object resp = in.readObject();
            if (resp instanceof Long valor) {
                return valor;
            }
        } catch (Exception e) {
            //StreamServer no disponible: se reporta como no disponible
        }
        return -1;
    }

    /*cada hilo de carga simula un streamer+viewer*/
    private void runLoad(int threadId) {
        String threadName = "Carga_" + threadId;
        String channelName = "canal_" + threadId;
        activeThreads.incrementAndGet();

        try {
            startLatch.await(); //esperar la senal de partida
        } catch (InterruptedException e) {
            return;
        }

        long endTime = System.currentTimeMillis() + TEST_DURATION_MS;

        while (System.currentTimeMillis() < endTime) {

            //ejercitar Ricart-Agrawala: crear canal (como streamer)
            long t1 = System.nanoTime();
            boolean ok = createChannel(threadName, channelName);
            metrics.recordRequest(System.nanoTime() - t1, ok);

            if (ok) {
                //ejercitar chat: suscribirse y enviar mensajes
                long t2 = System.nanoTime();
                boolean subOk = subscribeAndChat(threadName, channelName);
                metrics.recordRequest(System.nanoTime() - t2, subOk);
            }

            //pequena pausa para no saturar
            try { Thread.sleep(500); } catch (InterruptedException e) { break; }
        }
    }

    /*crea un canal como streamer (ejercita Ricart-Agrawala)*/
    private boolean createChannel(String streamerId, String channelName) {
        try {
            Socket socket = new Socket(HOST, STREAM_PORT);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            ObjectInputStream in = new SafeObjectInputStream(socket.getInputStream());

            StreamSession session = new StreamSession(streamerId, channelName,
                "Canal de prueba " + channelName, "Prueba",
                Arrays.asList("test", "carga"));
            out.writeObject(session);
            out.flush();

            ServerResponse resp = (ServerResponse) in.readObject();
            socket.close();
            return resp.getStatus().name().equals("OK");

        } catch (Exception e) {
            return false;
        }
    }

    /*se suscribe a un canal y envia mensajes de chat*/
    private boolean subscribeAndChat(String username, String channelName) {
        try {
            //1: suscribirse al canal via StreamServer
            Socket streamSocket = new Socket(HOST, STREAM_PORT);
            ObjectOutputStream streamOut = new ObjectOutputStream(streamSocket.getOutputStream());
            streamOut.flush();
            ObjectInputStream streamIn = new SafeObjectInputStream(streamSocket.getInputStream());

            streamOut.writeObject(channelName);
            streamOut.flush();
            streamIn.readObject(); //ServerResponse
            streamSocket.close();

            //2: conectar al chat y enviar mensajes
            Socket chatSocket = new Socket(HOST, CHAT_PORT);
            ObjectOutputStream chatOut = new ObjectOutputStream(chatSocket.getOutputStream());
            chatOut.flush();
            ObjectInputStream chatIn = new SafeObjectInputStream(chatSocket.getInputStream());

            //mensaje de entrada
            chatOut.writeObject(new ChatMessage(username, channelName, "se ha unido a la carga"));
            chatOut.flush();

            //enviar algunos mensajes
            for (int i = 0; i < 3; i++) {
                chatOut.writeObject(new ChatMessage(username, channelName,
                    "Mensaje de carga #" + i + " desde " + username));
                chatOut.flush();
                try { Thread.sleep(100); } catch (InterruptedException e) { break; }
            }

            chatSocket.close();
            return true;

        } catch (Exception e) {
            return false;
        }
    }

    /*envia DIE al CoordinatorNode y mide recuperacion*/
    private void sendDie() {
        try { Thread.sleep(DIE_AFTER_MS); }
        catch (InterruptedException e) { return; }

        System.out.println("\n[Carga] Enviando DIE al CoordinatorNode...");
        fallaTime = System.currentTimeMillis();
        fallaInducida = true;

        try {
            Socket socket = new Socket(HOST, COORD_PORT);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            ObjectInputStream in = new SafeObjectInputStream(socket.getInputStream());
            MessageWithClock dieMsg = new MessageWithClock(0, "LoadGenerator", "DIE", null);
            out.writeObject(dieMsg);
            out.flush();
            in.readObject();
            socket.close();
            System.out.println("[Carga] DIE enviado. Midiendo recuperacion...");
        } catch (Exception e) {
            System.err.println("[Carga] Error al enviar DIE: " + e.getMessage());
            return;
        }

        //esperar a que las peticiones recientes tengan latencia normal
        long[] ultimasLatencias = new long[20];
        int idx = 0;
        for (int i = 0; i < 30; i++) {
            try { Thread.sleep(500); } catch (InterruptedException e) { break; }

            //probar si el sistema responde haciendo una peticion rapida
            long t1 = System.nanoTime();
            try (Socket s = new Socket(HOST, STREAM_PORT)) {
                ObjectOutputStream o = new ObjectOutputStream(s.getOutputStream()); o.flush();
                ObjectInputStream in = new SafeObjectInputStream(s.getInputStream());
                o.writeObject("LIST_CHANNELS"); o.flush();
                in.readObject();
                long lat = (System.nanoTime() - t1) / 1_000_000;
                ultimasLatencias[idx % 20] = lat;
                idx++;
            } catch (Exception e) {
                ultimasLatencias[idx % 20] = 9999; //fallo cuenta como latencia alta
                idx++;
            }

            //promedio de las ultimas 20 peticiones
            int count = Math.min(idx, 20);
            long sum = 0;
            for (int j = 0; j < count; j++) sum += ultimasLatencias[j];
            double avg = (double) sum / count;

            if (avg < 800 && count >= 5) {
                recoveryTime = System.currentTimeMillis();
                System.out.println("[Carga] Sistema recuperado en ~" + (recoveryTime - fallaTime) + "ms");
                return;
            }
        }
        recoveryTime = System.currentTimeMillis();
        System.out.println("[Carga] Espera agotada. Recovery = " + (recoveryTime - fallaTime) + "ms");
    }
}
