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
}
