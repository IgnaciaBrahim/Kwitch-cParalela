package cpyd.loadtest;

import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;


/*
Lleva la cuenta de cuantas peticiones se hicieron, cuantas fallaron,
cuantas tardaron, y cuantos mensajes de coordinacion (REQUEST/REPLY/
RELEASE) se generaron.

Los datos se guardan en colas concurrentes para que varios hilos
puedan usarlo sin problemas.
*/

public class MetricsCollector {

    private final ConcurrentLinkedQueue<Long> latencies = new ConcurrentLinkedQueue<>();
    private final AtomicLong totalRequests = new AtomicLong(0);
    private final AtomicLong failedRequests = new AtomicLong(0);
    private final AtomicLong coordinationMessages = new AtomicLong(0);
    private final long startTime;

    //cada muestra ademas guarda el instante (epoch ms) en que se completo,
    //para poder separar despues las peticiones de antes/durante/despues de
    //una falla inducida. No se usa en getThroughput/getAvgLatency/getP95 ni
    //en printReport(), asi que no cambia ninguno de esos resultados.
    private final ConcurrentLinkedQueue<long[]> samples = new ConcurrentLinkedQueue<>();

    public MetricsCollector() {
        this.startTime = System.currentTimeMillis();
    }

    //llamar cuando se completa una peticion, con cuanto tardó en nanosegundos
    public void recordRequest(long latencyNanos, boolean success) {
        totalRequests.incrementAndGet();
        latencies.add(latencyNanos);
        if (!success) {
            failedRequests.incrementAndGet();
        }
        samples.add(new long[]{System.currentTimeMillis(), latencyNanos, success ? 1 : 0});
    }

    //cuenta un mensaje de coordinacion (REQUEST, REPLY o RELEASE)
    public void recordCoordinationMessage() {
        coordinationMessages.incrementAndGet();
    }

    //peticiones por segundo desde el inicio
    public double getThroughput() {
        long elapsed = System.currentTimeMillis() - startTime;
        if (elapsed == 0) return 0;
        return (double) totalRequests.get() / (elapsed / 1000.0);
    }

    //latencia promedio en milisegundos
    public double getAvgLatency() {
        long sum = 0;
        int count = 0;
        for (long lat : latencies) {
            sum += lat;
            count++;
        }
        if (count == 0) return 0;
        //convertir nanosegundos a ms
        return (sum / (double) count) / 1_000_000.0;
    }

    //percentil 95 de latencia en milisegundos
    public double getP95() {
        List<Long> sorted = new ArrayList<>(latencies);
        if (sorted.isEmpty()) return 0;
        Collections.sort(sorted);
        int index = (int) Math.ceil(0.95 * sorted.size()) - 1;
        return sorted.get(Math.max(0, index)) / 1_000_000.0;
    }

    //porcentaje de peticiones que fallaron
    public double getErrorRate() {
        long total = totalRequests.get();
        if (total == 0) return 0;
        return (double) failedRequests.get() / total * 100.0;
    }

    //total de mensajes de coordinacion enviados
    public long getCoordinationMsgCount() {
        return coordinationMessages.get();
    }

    //cuantas peticiones se hicieron en total
    public long getTotalRequests() {
        return totalRequests.get();
    }

    //muestra todo en consola
    public void printReport() {
        System.out.println("\n========== REPORTE DE MÉTRICAS ==========");
        System.out.println("Peticiones totales:      " + totalRequests.get());
        System.out.println("Throughput:              " + String.format("%.2f", getThroughput()) + " req/s");
        System.out.println("Latencia promedio:       " + String.format("%.2f", getAvgLatency()) + " ms");
        System.out.println("Latencia P95:            " + String.format("%.2f", getP95()) + " ms");
        System.out.println("Tasa de error:           " + String.format("%.2f", getErrorRate()) + "%");
        System.out.println("Mensajes coordinación:   " + coordinationMessages.get());
        System.out.println("==========================================\n");
    }

    /*separa las peticiones en tres ventanas de tiempo (antes de la falla,
    durante la caida del nodo, despues de la recuperacion) y muestra el
    throughput/latencia/error de cada una por separado.

    Es un detalle adicional a printReport(): en el promedio global de toda
    la prueba el impacto de la falla casi no se nota porque son pocas
    peticiones frente al total, y por eso conviene mostrarlo aparte.*/
    public void printPhaseReport(long failTimeMillis, long recoveryTimeMillis) {
        List<long[]> antes = new ArrayList<>();
        List<long[]> durante = new ArrayList<>();
        List<long[]> despues = new ArrayList<>();

        for (long[] s : samples) {
            long ts = s[0];
            if (ts < failTimeMillis) {
                antes.add(s);
            } else if (ts <= recoveryTimeMillis) {
                durante.add(s);
            } else {
                despues.add(s);
            }
        }

        System.out.println("\n===== METRICAS POR VENTANA (impacto de la falla inducida) =====");
        imprimirVentana("ANTES de la falla    ", antes);
        imprimirVentana("DURANTE la caida     ", durante);
        imprimirVentana("DESPUES de recuperado", despues);
        System.out.println("=================================================================\n");
    }

    private void imprimirVentana(String nombre, List<long[]> muestras) {
        if (muestras.isEmpty()) {
            System.out.println(nombre + " -> sin peticiones registradas en esta ventana");
            return;
        }

        long total = muestras.size();
        long fallidas = 0;
        List<Long> lats = new ArrayList<>();
        for (long[] s : muestras) {
            if (s[2] == 0) fallidas++;
            lats.add(s[1]);
        }
        Collections.sort(lats);

        double errorRate = (double) fallidas / total * 100.0;
        long sum = 0;
        for (long lat : lats) sum += lat;
        double avgMs = (sum / (double) total) / 1_000_000.0;
        int idx = (int) Math.ceil(0.95 * lats.size()) - 1;
        double p95Ms = lats.get(Math.max(0, idx)) / 1_000_000.0;

        System.out.println(nombre + " -> peticiones=" + total
            + " | error=" + String.format("%.2f", errorRate) + "%"
            + " | latencia avg=" + String.format("%.2f", avgMs) + "ms"
            + " | p95=" + String.format("%.2f", p95Ms) + "ms");
    }
}
