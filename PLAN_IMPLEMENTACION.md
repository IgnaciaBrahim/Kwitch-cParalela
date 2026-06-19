# Plan de Implementación — Paso a Paso

Basado en la retroalimentación recibida y el estado actual del código.

---

## Orden de implementación (de más fácil a más difícil)

### Paso 1: Conectar LamportClock al chat (2.2)
**Archivos a modificar:** `ChatServer.java`, `ChatClientHandler.java`
**Archivos nuevos:** ninguno

**Qué hacer:**
- En `ChatServer.java`: agregar `LamportClock clock = new LamportClock();`
- En `ChatClientHandler.java`:
  - Al recibir un `ChatMessage`, envolverlo en `MessageWithClock` con `clock.tick()`
  - Al recibir del socket, extraer timestamp y hacer `clock.update(msg.lamportTime)`
  - Meter mensajes en `PriorityBlockingQueue` ordenada por (lamportTime, senderId)
  - Entregar al viewer en ese orden
- Escribir log cada evento: `[LAMPORT=X] [CHAT] user: mensaje`

**Tiempo estimado:** ~30 min

---

### Paso 2: HeartbeatMonitor (2.4)
**Archivos nuevos:** `distributed/HeartbeatMonitor.java`

**Qué hacer:**
- Clase con:
  - `LamportClock clock`
  - `NodeMembership membership`
  - `List<NodeInfo> peers` — los otros nodos a monitorear
  - `int heartbeatInterval = 5000` (5s)
  - `int timeout = 15000` (15s)
- Hilo 1 (sender): cada 5s envía `MessageWithClock(clock.tick(), myId, "HEARTBEAT", null)` a cada peer por TCP
- Hilo 2 (receiver): `ServerSocket` escuchando en puerto designado, recibe heartbeats, actualiza `lastHeartbeat[nodoId]`
- Hilo 3 (checker): cada 5s revisa si algún nodo superó el timeout → `markFailed(id)`

**Tiempo estimado:** ~45 min

---

### Paso 3: CoordinatorNode (2.1)
**Archivos nuevos:** `distributed/CoordinatorNode.java`

**Qué hacer:**
- ServerSocket en puerto 7000
- `NodeMembership membership` — recibe registros de StreamServer y ChatServer
- `HeartbeatMonitor` — monitorea a los otros 2 nodos
- `LamportClock clock`
- Cuando recibe `MessageWithClock("REGISTER", payload=NodeInfo)` → `membership.registerNode(...)`
- Cuando recibe `MessageWithClock("MEMBERSHIP")` → responde con la lista de nodos
- Cuando recibe `MessageWithClock("DIE")` → `System.exit(0)` (para la falla inducida)
- Participa en Ricart-Agrawala como nodo de voto

**Tiempo estimado:** ~40 min

---

### Paso 4: RicartAgrawala (2.3)
**Archivos nuevos:** `distributed/RicartAgrawala.java`
**Archivos a modificar:** `StreamServer.java`, `CoordinatorNode.java`, `ChatServer.java`

**Qué hacer:**
- Clase con:
  - `LamportClock clock`
  - `NodeMembership membership`
  - `enum Estado { LIBRE, DESEADO, TOMADO }`
  - `int mySeq` — mi timestamp para la solicitud actual
  - `List<MessageWithClock> deferredReplies` — cola de diferidos
  - `Set<String> repliesReceived` — quienes ya respondieron
  - `int peersNeeded` — cuantos replies esperar (solo nodos ALIVE)
- **requestCS():**
  1. `estado = DESEADO`
  2. `mySeq = clock.tick()`
  3. `repliesReceived.clear()`
  4. `peersNeeded = membership.getAliveNodes().size() - 1` (todos menos yo)
  5. Multicast `MessageWithClock(mySeq, myId, "REQUEST", null)` a vivos
  6. Esperar hasta `repliesReceived.size() >= peersNeeded`
  7. `estado = TOMADO`
- **handleRequest(msg):**
  1. Si LIBRE → enviar REPLY
  2. Si TOMADO → guardar en deferred
  3. Si DESEADO → comparar prioridad (miSeq, miID) vs (msg.lamportTime, msg.senderId). Si él gana → REPLY, si yo gano → diferir
- **releaseCS():**
  1. `estado = LIBRE`
  2. Enviar REPLY a todos los diferidos
  3. `deferredReplies.clear()`
- **handleReply():** agregar a `repliesReceived`
- **handleRelease():** enviar REPLY pendientes
- **Integrar con Heartbeat:** cuando `markFailed(id)`, decrementar `peersNeeded` y despertar si ya tenemos suficientes

**Tiempo estimado:** ~90 min

---

### Paso 5: Conectar Ricart-Agrawala a StreamServer (2.3 + 2.1)
**Archivos a modificar:** `StreamServer.java`, `CoordinatorNode.java`

**Qué hacer:**
- `StreamServer.java`:
  - Crear `RicartAgrawala` al iniciar
  - En `handleStreamer()`, antes de crear canal: `ricart.requestCS()`, después: `ricart.releaseCS()`
  - Iniciar `HeartbeatMonitor`
  - Registrar con CoordinatorNode
- `CoordinatorNode.java`:
  - Crear `RicartAgrawala`
  - Responder a REQUEST/REPLY/RELEASE como cualquier nodo
- `ChatServer.java`:
  - Iniciar `HeartbeatMonitor`
  - Registrar con CoordinatorNode

---

### Paso 6: LoadGenerator (3.1 + 3.2 + 3.3)
**Archivos nuevos:** `loadtest/LoadGenerator.java`

**Qué hacer:**
- `ExecutorService pool = Executors.newFixedThreadPool(50)`
- `MetricsCollector metrics = new MetricsCollector()`
- `CountDownLatch startLatch = new CountDownLatch(1)` — para sincronizar todos
- Cada tarea:
  1. `startLatch.await()` — espera la señal
  2. Bucle por 65 segundos:
     - `fetchChannels()` → medir latencia, registrar en metrics
     - `subscribe(channel)` → medir latencia
     - `sendChatMessage()` → medir latencia
     - Si falla → `metrics.recordRequest(latency, false)`
- Hilo aparte que a los ~30s envía `MessageWithClock("DIE")` al puerto 7000
- Hilo aparte que monitorea latencia cada segundo para detectar cuándo se estabiliza (tiempo recuperación)
- Al final: `metrics.printReport()`

**Tiempo estimado:** ~60 min

---

## Resumen de archivos

### Archivos nuevos (5):

| Archivo | Líneas | Paso |
|---|---|---|
| `distributed/HeartbeatMonitor.java` | ~70 | 2 |
| `distributed/CoordinatorNode.java` | ~100 | 3 |
| `distributed/RicartAgrawala.java` | ~150 | 4 |
| `loadtest/LoadGenerator.java` | ~150 | 6 |

### Archivos a modificar (3):

| Archivo | Cambio | Paso |
|---|---|---|
| `server/StreamServer.java` | Agregar LamportClock, HeartbeatMonitor, RicartAgrawala | 1, 2, 4, 5 |
| `server/ChatServer.java` | Agregar LamportClock, HeartbeatMonitor, orden causal | 1, 2 |
| `handler/ChatClientHandler.java` | Envolver mensajes en MessageWithClock, ordenar entrega | 1 |

---

## Reglas de Lamport (recordatorio)

| Regla | Acción |
|---|---|
| **LC1** | Antes de enviar: `reloj = reloj + 1`, adjuntar ese valor |
| **LC2** | Al recibir: `reloj = Math.max(reloj, marcaRecibida) + 1` |
| **Desempate** | Si mismo timestamp, menor ID de nodo tiene prioridad |

---

## Integración Heartbeat + Ricart-Agrawala (crítico)

Ricart-Agrawala es sensible a caídas: si esperas REPLY de un nodo caído, el sistema se deadlockea.

**Solución:** Antes de enviar REQUEST, calcular `peersNeeded = membership.getAliveNodes().size() - 1`. Cuando Heartbeat marca un nodo como FAILED, si estamos esperando su REPLY, decrementar `peersNeeded` y verificar si ya podemos entrar a CS.
