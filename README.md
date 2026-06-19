# Kwitch — Plataforma de Streaming Distribuida

Plataforma distribuida en Java que replica las funciones de Twitch (streaming + chat en tiempo real). Proyecto universitario para Computación Paralela y Distribuida.

---

## Cómo compilar

```bash
cd kwitch-cparalela
mvn compile
```

## Cómo ejecutar (5 terminales, en este orden)

```bash
# 1. Servidor de streams (obligatorio)
java -cp target/classes cpyd.server.StreamServer
# 2. Servidor de chat (obligatorio)
java -cp target/classes cpyd.server.ChatServer
# 3. Coordinador de nodos (obligatorio)
java -cp target/classes cpyd.distributed.CoordinatorNode
# 4. Cliente del streamer
java -cp target/classes cpyd.client.StreamerClient
# 5. Cliente del viewer
java -cp target/classes cpyd.client.ViewerClient
```

Prueba de carga (requiere los 3 servidores corriendo):
```bash
java -cp target/classes cpyd.loadtest.LoadGenerator
```

---

## Estructura del proyecto — qué hace cada archivo

### `model/` — Objetos que viajan por la red (DTOs serializables)

| Archivo | Qué hace | Si hay error en... |
|---|---|---|
| `StreamSession.java` | Define un canal: nombre, streamer, tags, estado (LIVE/PAUSED/ENDED), duración, viewers | La creación o actualización de canales |
| `ChatMessage.java` | Define un mensaje de chat: usuario, canal, contenido, timestamp | El envío o recepción de mensajes |
| `ServerResponse.java` | Respuesta del servidor: status (OK/ERROR/CHANNEL_CLOSED), mensaje, payload | Las respuestas que reciben los clientes |
| `StreamStatus.java` | Enum: LIVE, PAUSED, ENDED | El ciclo de vida del stream |
| `ResponseStatus.java` | Enum: OK, ERROR, CHANNEL_CLOSED | Los códigos de respuesta |

### `server/` — Servidores TCP (escuchan conexiones entrantes)

| Archivo | Qué hace | Si hay error en... |
|---|---|---|
| `StreamServer.java` | Puerto 5000. Acepta conexiones de streamers y viewers. Tiene los mapas `activeSessions` y `subscribers`. Registra los 3 nodos en la membresía, inicia HeartbeatMonitor y RicartAgrawala | La conexión de clientes al puerto 5000, el registro de canales |
| `ChatServer.java` | Puerto 6000. Acepta conexiones de chat. Tiene reloj Lamport, HeartbeatMonitor y RicartAgrawala (votante). Registra con CoordinatorNode | La conexión de viewers al chat, el broadcast de mensajes |

### `handler/` — Lógica por cada cliente conectado

| Archivo | Qué hace | Si hay error en... |
|---|---|---|
| `ClientHandler.java` | Recibe la conexión y decide si es STREAMER (llega un StreamSession) o VIEWER (llega un String). Los streamers pasan por Ricart-Agrawala antes de crear el canal. Los viewers se suscriben y reciben broadcast | La creación de canales, la suscripción de viewers, la desconexión de clientes |
| `ChatClientHandler.java` | Maneja el chat de un viewer. Aplica LC1/LC2 de Lamport, encola mensajes en PriorityBlockingQueue y los entrega ordenados por timestamp | El orden de los mensajes, la conexión/desconexión del chat |

### `client/` — Interfaces de consola

| Archivo | Qué hace | Si hay error en... |
|---|---|---|
| `StreamerClient.java` | Pide datos del canal por consola, envía StreamSession al servidor, muestra menú (pausar/reanudar/cerrar) | La interacción del streamer, el envío de comandos |
| `ViewerClient.java` | Pide nombre de usuario, muestra canales disponibles, se suscribe, abre chat. Tiene reconexión automática al StreamServer | La experiencia del viewer, la reconexión |

### `distributed/` — Algoritmos del sistema distribuido

| Archivo | Qué hace | Si hay error en... |
|---|---|---|
| `LamportClock.java` | Reloj lógico: `tick()` (LC1: antes de enviar), `update(n)` (LC2: al recibir), `getTime()` | Los timestamps Lamport, el orden causal |
| `MessageWithClock.java` | Envuelve cualquier mensaje con timestamp, senderId, type y payload. Es el formato estándar entre nodos | La comunicación entre servidores |
| `NodeMembership.java` | Mapa de nodos con estado ALIVE/FAILED. Métodos: registerNode, markFailed, markAlive, getAliveNodes | La detección de nodos caídos, la membresía |
| `NodeLogger.java` | Escribe logs a `logs/node_Nombre.log` y también a consola. Tiene `log()`, `error()`, `logLamport()` | Los logs que se entregan en el informe |
| `HeartbeatMonitor.java` | 3 hilos: sender (cada 5s manda HEARTBEAT), receiver (ServerSocket, recibe HEARTBEAT), checker (cada 5s revisa timeout de 15s, marca FAILED) | La detección de caídas de servidores |
| `RicartAgrawala.java` | Exclusión mutua distribuida. Estados: LIBRE, DESEADO, TOMADO. requestCS() pide permiso a nodos activos, releaseCS() libera. Usa timestamps Lamport para prioridad | La coordinación entre nodos para crear canales |
| `CoordinatorNode.java` | Puerto 7000. Recibe REGISTER (nuevos nodos), MEMBERSHIP (consulta de activos), DIE (apagado inducido). Participa como votante en Ricart-Agrawala | La tercera terminal, el registro de nodos, la falla inducida |

### `loadtest/` — Prueba de carga

| Archivo | Qué hace | Si hay error en... |
|---|---|---|
| `LoadGenerator.java` | 50 hilos, 65 segundos. Cada hilo crea un canal (ejercita RA), se suscribe y envía chat. A los 30s manda DIE al CoordinatorNode y mide recuperación | La prueba de carga, las métricas |
| `MetricsCollector.java` | Cuenta throughput (req/s), latencia promedio y P95, tasa de error, mensajes de coordinación. Método `printReport()` | Los resultados de la prueba |

---

## Puertos

| Nodo | Clientes | Heartbeat | Ricart-Agrawala |
|---|---|---|---|
| StreamServer | 5000 | 5001 | 5002 |
| ChatServer | 6000 | 6001 | 6002 |
| CoordinatorNode | 7000 | 7001 | 7002 |

---

## Conceptos distribuidos

| Concepto | Resumen |
|---|---|
| **3+ nodos** | StreamServer:5000, ChatServer:6000, CoordinatorNode:7000 como procesos independientes |
| **Membresía** | Los 3 nodos se registran mutuamente al iniciar. Además envían REGISTER al CoordinatorNode y re-registran cada 30s |
| **Lamport** | LC1: `clock.tick()` antes de enviar. LC2: `clock.update(n)` al recibir. PriorityBlockingQueue ordena por (timestamp, senderId) |
| **Ricart-Agrawala** | requestCS() multicasts REQUEST a `getAliveNodes()`, espera REPLY, entra a CS. releaseCS() envía REPLY a diferidos |
| **Heartbeat** | Sender cada 5s, receiver con ServerSocket, checker cada 5s con timeout 15s → `markFailed()` |
| **Recuperación** | Heartbeat entrante → `markAlive()`. Re-registro cada 30s al CoordinatorNode |
| **Falla inducida** | DIE al puerto 7000. LoadGenerator mide recuperación con rolling window de 20 latencias |
