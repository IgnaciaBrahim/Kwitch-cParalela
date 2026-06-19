# Checklist vs Proyecto Final ICI4344.md — Estado Actual

---

## Resumen de archivos (20 fuentes)

```
cpyd/
├── model/     → StreamSession, ChatMessage, ServerResponse, StreamStatus, ResponseStatus
├── server/    → StreamServer, ChatServer
├── handler/   → ClientHandler, ChatClientHandler
├── client/    → StreamerClient, ViewerClient
├── distributed/  → LamportClock, MessageWithClock, NodeMembership,
│                   NodeLogger, HeartbeatMonitor, CoordinatorNode, RicartAgrawala
└── loadtest/     → MetricsCollector
```

---

## 2.1 Topología multinodo — 3 nodos servidores

**Estado: COMPLETO ✅**

**Cumple con:**
- 3 nodos servidores: `StreamServer` (5000), `ChatServer` (6000), `CoordinatorNode` (7000) - cada uno con su `main()` y `ServerSocket`
- `CoordinatorNode.java:25-71` — ServerSocket en puerto 7000, registra nodos (REGISTER), responde membresía (MEMBERSHIP), recibe DIE
- `NodeMembership.java` — ConcurrentHashMap con estado ALIVE/FAILED, usado por los 3 servidores
- `StreamServer.java:72-85` y `ChatServer.java:73-86` — se registran con CoordinatorNode al iniciar
- Heartbeats entre todos los nodos (puertos 5001/6001/7001)
- Ricart-Agrawala entre todos los nodos (puertos 5002/6002/7002)
- Comunicación inter-servidor a través de RA y HB puertos dedicados
- Sockets TCP + serialización (`MessageWithClock`) en todas las comunicaciones

**Qué falta:** Nada esencial. Opcional: hardcodear IPs configurables en vez de "localhost".

---

## 2.2 Ordenamiento de eventos (Lamport)

**Estado: COMPLETO ✅**

**Cumple con:**
- `LamportClock.java` — `tick()`, `update()`, `getTime()` sincronizados
- `MessageWithClock.java` — wrapper serializable con `lamportTime`, `senderId`, `type`, `payload`
- **LC1 aplicada** en `ChatClientHandler.java:82`: `int ts = server.getClock().tick()` antes de broadcast
- **LC2 aplicada** en:
  - `ChatClientHandler.java:106`: `server.getClock().update(msg.getLamportTime())` al recibir
  - `HeartbeatMonitor.java:135`: `clock.update(msg.getLamportTime())` al recibir HEARTBEAT
  - `RicartAgrawala.java` en receiverLoop y handleRequest
  - `CoordinatorHandler.java:89`: al recibir cualquier mensaje
- `NodeLogger.java` — escribe `logs/node_*.log` con `[LAMPORT=X] [NODO] evento`
- Logs Lamport en chat: `ChatClientHandler.java:85` usa `server.getLogger().logLamport(ts, ...)`
- Logs RA: `RicartAgrawala.java` usa `logger.log()` para todos los eventos

**Qué falta:** Ordenación por cola (PriorityBlockingQueue) para garantizar orden de entrega al viewer. Hoy se entregan en orden de llegada, no estrictamente por Lamport.

---

## 2.3 Coordinación distribuida (Ricart-Agrawala)

**Estado: COMPLETO ✅**

**Cumple con:**
- `RicartAgrawala.java` completo con 3 estados: LIBRE, DESEADO, TOMADO
- `requestCS()`: estado=DESEADO, `mySeq=clock.tick()`, envía REQUEST a peers ACTIVOS, espera REPLY de todos
- `handleRequest()`: compara prioridad (timestamp Lamport, desempata por ID), difiere o responde REPLY
- `releaseCS()`: estado=LIBRE, envía REPLY a todos los diferidos
- Protege recurso crítico: `ClientHandler.java:114-125` envuelve `activeSessions.put()` con `ricart.requestCS()`/`releaseCS()`
- Integrado con Heartbeat: `requestCS()` consulta `membership.getAliveNodes()` para ignorar nodos FAILED
- Los 3 nodos participan: StreamServer (pide CS), ChatServer y CoordinatorNode (votan)

**Qué falta:** Nada. El algoritmo está completo.

---

## 2.4 Tolerancia a fallos

**Estado: COMPLETO ✅**

**Cumple con:**
- `HeartbeatMonitor.java` — 3 hilos daemon por nodo:
  - Sender: cada 5s envía `MessageWithClock(HEARTBEAT)` a peers
  - Receiver: ServerSocket escuchando en puerto HB, recibe HEARTBEAT, actualiza lastHeartbeat
  - Checker: cada 5s revisa si algún peer superó 15s sin heartbeat → `membership.markFailed(id)`
  - Reintegración automática: al recibir HEARTBEAT de un nodo FAILED → `markAlive(id)`
- Timeouts en handlers: `setSoTimeout(180000)` en `ClientHandler.java:57`, `setSoTimeout(60000)` en `ChatClientHandler.java:45`
- Crash detection: catch `SocketException | EOFException` en todos los handlers
- Cleanup en finally: handlers remueven suscripciones al desconectarse
- Reconexión viewer: `while(true)` con sleep 5s en `ViewerClient.java:108-135`
- Ricart-Agrawala no se deadlockea: ignora nodos FAILED y solo espera REPLY de nodos ALIVE

**Qué falta:** Nada esencial. La reconexión del viewer usa sleep fijo (no backoff exponencial).

---

## 3.1 Generador de carga

**Estado: ❌ NO IMPLEMENTADO**

**Cumple con:**
- Nada

**Qué falta:**
- `LoadGenerator.java` completo: 50+ hilos con `Executors.newFixedThreadPool(50)`, 60+ segundos
- `CountDownLatch` para partida sincronizada de todos los hilos
- Cada hilo: fetchChannels → subscribe → sendChatMessage → medir latencia
- Usar `System.nanoTime()` para medir RTT de cada operación

---

## 3.2 Métricas

**Estado: ⚠️ PARCIAL**

**Cumple con:**
- `MetricsCollector.java` — throughput, latencia avg/p95, error rate, mensajes coordinación
- `RicartAgrawala.java` llama `metrics.recordCoordinationMessage()` en cada REQUEST/REPLY/RELEASE
- `StreamServer.java:48` — `MetricsCollector metrics` creado y pasado a RicartAgrawala

**Qué falta:**
- MetricsCollector no está siendo alimentado con latencias de operaciones (falta LoadGenerator)
- No hay medición de throughput real (necesita carga)
- getP95() recorre toda la lista cada vez (ineficiente, pero ok para proyecto estudiante)

---

## 3.3 Falla inducida

**Estado: ⚠️ PARCIAL**

**Cumple con:**
- `CoordinatorNode.java:124-134` — recibe `MessageWithClock("DIE")` y hace `System.exit(0)`
- Los nodos restantes (StreamServer, ChatServer) detectan la caída via HeartbeatMonitor (timeout 15s)
- Ricart-Agrawala continúa ignorando al nodo FAILED

**Qué falta:**
- LoadGenerator debería enviar el DIE (~segundo 30 de carga)
- No hay medición de `recuperacion_ms` (tiempo hasta que latencia se estabiliza)
- No hay logs automáticos del evento de falla (se vería en los logs de HeartbeatMonitor)

---

## 4.4 Distribución y Comunicación

**Estado: COMPLETO ✅**

**Cumple con:**
- 5 procesos (StreamServer, ChatServer, CoordinatorNode, StreamerClient, ViewerClient)
- Sockets TCP + ObjectOutputStream/ObjectInputStream en todos
- Marshalling: StreamSession, ChatMessage, ServerResponse, MessageWithClock serializables
- NodeMembership con registro y consulta de nodos activos
- Comunicación inter-servidor por puertos dedicados (HB y RA)
- 3 servidores con ConcurrentHashMap + CopyOnWriteArrayList thread-safe

---

## 4.5 Coordinación y Ordenamiento

**Estado: COMPLETO ✅**

**Cumple con:**
- LamportClock implementado en todos los nodos
- MessageWithClock usado en chat, heartbeats y Ricart-Agrawala
- RicartAgrawala completo con verificación de prioridad
- Concurrencia thread-safe: ConcurrentHashMap, CopyOnWriteArrayList, ExecutorService, synchronized en LamportClock

---

## 4.6 Tolerancia a Fallos y Funciones

**Estado: COMPLETO ✅** (sin carga)

**Cumple con:**
- Función 1 (streaming): StreamerClient + StreamServer + ClientHandler operativa
- Función 2 (chat): ViewerClient + ChatServer + ChatClientHandler operativa
- Heartbeat-based failure detection entre servidores
- Detección de crash (SocketException) y omisión (SocketTimeoutException)
- Manejo excepciones de red en todos los handlers
- Cleanup en finally blocks
- Reconexión automática de viewers

**Qué falta:**
- Probar bajo carga (necesita LoadGenerator)
- Backoff exponencial en reconexión de viewer (opcional)

---

## Resumen Final

| Requisito | Estado | Archivos clave |
|---|---|---|
| **2.1** Topología multinodo | ✅ Completo | `CoordinatorNode`, `NodeMembership`, `StreamServer`, `ChatServer` |
| **2.2** Ordenamiento eventos | ✅ Completo | `LamportClock`, `MessageWithClock`, `NodeLogger`, `ChatClientHandler` |
| **2.3** Coordinación distribuida | ✅ Completo | `RicartAgrawala`, `ClientHandler` |
| **2.4** Tolerancia a fallos | ✅ Completo | `HeartbeatMonitor`, handlers con timeout |
| **3.1** Generador de carga | ❌ No existe | Falta `LoadGenerator` |
| **3.2** Métricas | ⚠️ Parcial | `MetricsCollector` listo, sin carga que lo alimente |
| **3.3** Falla inducida | ⚠️ Parcial | `DIE` implementado, falta LoadGenerator que lo dispare |
| **4.4** Distribución | ✅ Completo | Sockets + serialización + membresía entre 3 nodos |
| **4.5** Coordinación y Ordenamiento | ✅ Completo | Lamport + Ricart-Agrawala + concurrencia |
| **4.6** Tolerancia a Fallos | ✅ Completo | Heartbeat + timeouts + cleanup + reconexión |

**Única tarea pendiente:** `LoadGenerator.java` (que también conecta MetricsCollector y activa la falla inducida).
