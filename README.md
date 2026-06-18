# Kwitch — Plataforma de Streaming Distribuida

Kwitch es una plataforma distribuida desarrollada en Java que simula las funciones centrales de servicios de transmisión en vivo como Twitch. Fue desarrollada para el ramo **Computación Paralela y Distribuida**, con foco en la resolución práctica de desafíos propios de los sistemas distribuidos: transparencia de ubicación, sincronización de hilos y mitigación de fallos parciales.

La interfaz es deliberadamente minimalista (consola interactiva) para que el rigor de la ingeniería se concentre en los mecanismos de comunicación distribuida subyacentes.

---

## Funciones principales

| Función | Descripción |
|---|---|
| **Gestión de Streams** | Ciclo de vida completo de canales: el streamer crea, pausa, reanuda y cierra su transmisión; los viewers se suscriben y reciben actualizaciones en tiempo real. |
| **Chat en Tiempo Real** | Sistema de mensajería por canal: los viewers conectados a un mismo canal pueden enviarse mensajes que se distribuyen instantáneamente a todos los participantes. |

---

## Arquitectura del sistema

El sistema está compuesto por **4 procesos autónomos** que se comunican exclusivamente a través de la red mediante sockets TCP:

```
  ┌─────────────────┐          ┌──────────────────────────────────────┐
  │  StreamerClient │──────────►                                      │
  └─────────────────┘  :5000   │         StreamServer (:5000)         │
  ┌─────────────────┐          │  activeSessions + subscribers map    │
  │   ViewerClient  │──────────►                                      │
  │                 │          └──────────────────────────────────────┘
  │                 │
  │                 │          ┌──────────────────────────────────────┐
  │                 │──────────►       ChatServer (:6000)             │
  └─────────────────┘  :6000   │       channels map (por canal)       │
                               └──────────────────────────────────────┘
```

- **StreamServer** (puerto 5000): gestiona los canales activos y la distribución de actualizaciones de estado a los viewers suscritos.
- **ChatServer** (puerto 6000): gestiona el chat por canal, distribuyendo mensajes a todos los participantes del mismo canal.
- **StreamerClient**: cliente del streamer — publica y controla su transmisión.
- **ViewerClient**: cliente del viewer — se suscribe a un canal y participa en el chat con doble conexión simultánea (puerto 5000 y 6000).

---

## Tecnologías y conceptos aplicados

| Concepto | Implementación |
|---|---|
| **Sockets TCP** | `ServerSocket` / `Socket` para comunicación cliente-servidor persistente |
| **Serialización de objetos** | `ObjectInputStream` / `ObjectOutputStream` para transmitir `StreamSession`, `ChatMessage` y `ServerResponse` |
| **Thread-per-client** | `StreamServer` crea un hilo por conexión (`ClientHandler`) |
| **Thread pool** | `ChatServer` usa `ExecutorService.newCachedThreadPool()` para eficiencia en carga I/O |
| **Hilos daemon** | Listeners en `ViewerClient` y `StreamerClient` marcados con `setDaemon(true)` |
| **Colecciones thread-safe** | `ConcurrentHashMap`, `CopyOnWriteArrayList` para acceso concurrente sin bloqueos explícitos |
| **Fallos de crash** | Detectados via `SocketException` / `EOFException` → limpieza y desuscripción del cliente |
| **Fallos de omisión** | Detectados via `SocketTimeoutException` (180s en stream, 60s en chat) → limpieza |
| **Reconexión automática** | `ViewerClient` reintenta la conexión al `StreamServer` con backoff de 5 segundos |
| **Protocolo por primer mensaje** | `ClientHandler` determina si un cliente es STREAMER o VIEWER según el tipo del primer objeto recibido |

---

## Estructura del proyecto

```
Kwitch-cParalela/
├── README.md
├── DIAGRAMAS_UML.md
├── Documentacion.md
└── kwitch-cparalela/
    ├── pom.xml
    └── src/main/java/cpyd/
        ├── StreamServer.java        # Servidor de streams (puerto 5000)
        ├── ClientHandler.java       # Hilo por cliente: rol STREAMER o VIEWER
        ├── ChatServer.java          # Servidor de chat (puerto 6000)
        ├── ChatClientHandler.java   # Hilo por cliente de chat
        ├── StreamerClient.java      # Cliente consola del streamer
        ├── ViewerClient.java        # Cliente consola del viewer
        ├── StreamSession.java       # DTO del canal/stream
        ├── ChatMessage.java         # DTO del mensaje de chat
        ├── ServerResponse.java      # Wrapper de respuesta del servidor
        ├── StreamStatus.java        # Enum: LIVE, PAUSED, ENDED
        ├── ResponseStatus.java      # Enum: OK, ERROR, CHANNEL_CLOSED
        └── Main.java                # Placeholder (sin uso activo)
```

---

## Requisitos previos

- **Java 17** o superior
- **Maven 3.6** o superior

```bash
java -version   # debe mostrar 17 o mayor
mvn -version    # debe mostrar 3.6 o mayor
```

---

## Compilación

Desde la raíz del repositorio:

```bash
cd kwitch-cparalela
mvn compile
```

Los `.class` compilados quedarán en `target/classes/`.

---

## Ejecución

Se necesitan **4 terminales separadas**, en el siguiente orden:

### Terminal 1 — StreamServer
```bash
cd kwitch-cparalela
java -cp target/classes cpyd.StreamServer
```
> Escucha en el puerto **5000**. Debe iniciarse primero.

### Terminal 2 — ChatServer
```bash
cd kwitch-cparalela
java -cp target/classes cpyd.ChatServer
```
> Escucha en el puerto **6000**. Debe iniciarse antes que los clientes.

### Terminal 3 — StreamerClient
```bash
cd kwitch-cparalela
java -cp target/classes cpyd.StreamerClient
```
> El streamer ingresa los datos de su canal por consola.

### Terminal 4 — ViewerClient
```bash
cd kwitch-cparalela
java -cp target/classes cpyd.ViewerClient
```
> El viewer ve los canales disponibles, selecciona uno y participa en el chat.

---

## Flujo de uso típico

1. **StreamerClient** inicia y solicita por consola:
   - ID del streamer
   - Nombre del canal
   - Descripción
   - Aplicación/juego que se transmite
   - Tags del canal
2. El canal queda registrado como **LIVE** en el `StreamServer`.
3. **ViewerClient** inicia, consulta la lista de canales activos y selecciona uno.
4. El viewer queda suscrito al canal y conectado al chat.
5. El streamer puede desde su consola:
   - `1` → Pausar el stream (estado **PAUSED**; viewers son notificados)
   - `2` → Reanudar el stream (estado **LIVE**; viewers son notificados)
   - `3` → Terminar el stream (estado **ENDED**; viewers son notificados y desconectados)
6. Los viewers pueden escribir mensajes de chat que se distribuyen a todos los presentes en el canal.

---

## Manejo de fallos

| Tipo de fallo | Detección | Respuesta del sistema |
|---|---|---|
| **Crash del cliente** (desconexión abrupta) | `SocketException` / `EOFException` | El handler limpia la suscripción y cierra los streams I/O |
| **Fallo de omisión** (cliente silencioso) | `SocketTimeoutException` (180s stream / 60s chat) | Timeout dispara la limpieza como si fuera un crash |
| **Canal no existente** | Verificación con `containsKey()` | Servidor responde `ServerResponse(ERROR)` al viewer |
| **Caída del StreamServer** (vista desde viewer) | `IOException` en el hilo daemon | `ViewerClient` reintenta la conexión cada 5 segundos |
| **Canal cerrado** por el streamer | Estado `ENDED` en `StreamSession` | Broadcast de `CHANNEL_CLOSED` a todos los viewers suscritos |

---

## Autores

* DIEGO ALVARADO MONDACA (DEV 1)
* IGNACIA BRAHIM LARA (DEV 2)
* BÁRBARA OYARZO ALFARO (DEV 3)
* VICENTE PALMA LUCERO (DEV 4)
* JAVIER RETAMAL FREZ (DEV 5)
* ARIEL VILLAR SAN JOSÉ (DEV 6)
