# Kwitch — Análisis Completo del Proyecto

## 1. Descripción General

**Kwitch** es una plataforma de *streaming* en vivo distribuida, desarrollada en **Java 17** como proyecto universitario para el ramo **Computación Paralela y Distribuida**. Simula las funciones centrales de servicios como Twitch —gestión de canales en vivo y chat en tiempo real— utilizando exclusivamente **sockets TCP** y **serialización de objetos Java**.

La interfaz es de **consola/CLI** (sin interfaz gráfica), lo que permite concentrar el rigor de ingeniería en los mecanismos de comunicación distribuida: transparencia de ubicación, sincronización de hilos y mitigación de fallos parciales.

| Aspecto | Detalle |
|---|---|
| **Lenguaje** | Java 17 |
| **Build System** | Maven 3.6+ |
| **Dependencias externas** | Ninguna (solo JDK estándar) |
| **Comunicación** | Sockets TCP + Object Serialization |
| **Concurrencia** | Threads, ExecutorService, Colecciones thread-safe |
| **Licencia** | CC0 1.0 Universal (Dominio Público) |

---

## 2. Arquitectura del Sistema

El sistema se compone de **4 procesos Java independientes** que se comunican exclusivamente por red mediante **sockets TCP** en dos puertos locales:

```mermaid
graph TB
    subgraph "Procesos del Sistema"
        SS[StreamServer\n:5000]
        CS[ChatServer\n:6000]
        SC[StreamerClient]
        VC[ViewerClient]
    end

    SC -- "StreamSession (TCP :5000)" --> SS
    VC -- "String/LIST_CHANNELS (TCP :5000)" --> SS
    VC -- "ChatMessage (TCP :6000)" --> CS
    SS -- "ServerResponse (TCP :5000)" --> SC
    SS -- "ServerResponse (TCP :5000)" --> VC
    CS -- "ChatMessage (TCP :6000)" --> VC

    style SS fill:#4a90d9,color:#fff
    style CS fill:#4a90d9,color:#fff
    style SC fill:#7ed321,color:#000
    style VC fill:#f5a623,color:#000
```

### 2.1 Procesos y Puertos

| Proceso | Puerto | Rol |
|---|---|---|
| **StreamServer** | 5000 | Gestiona canales activos y notificaciones de estado a viewers |
| **ChatServer** | 6000 | Gestiona mensajería por canal |
| **StreamerClient** | — | Interfaz del streamer: publica y controla su transmisión |
| **ViewerClient** | — | Interfaz del viewer: se suscribe a un canal y participa en el chat |

### 2.2 Flujo de Datos entre Procesos

```mermaid
sequenceDiagram
    participant SC as StreamerClient
    participant SS as StreamServer (:5000)
    participant VC as ViewerClient
    participant CS as ChatServer (:6000)

    Note over SC,CS: 1. Streamer abre canal
    SC->>SS: StreamSession (LIVE)
    SS-->>SC: ServerResponse(OK)

    Note over SC,CS: 2. Viewer descubre canales
    VC->>SS: "LIST_CHANNELS"
    SS-->>VC: List<String> canales

    Note over SC,CS: 3. Viewer se suscribe
    VC->>SS: "nombre_canal"
    SS-->>VC: ServerResponse(OK) + StreamSession

    Note over SC,CS: 4. Viewer conecta al chat
    VC->>CS: ChatMessage("se ha unido")
    CS-->>VC: ChatMessage(otros usuarios)

    Note over SC,CS: 5. Streamer actualiza estado
    SC->>SS: StreamSession(PAUSED/LIVE/ENDED)
    SS-->>VC: ServerResponse + StreamSession (broadcast)
```

---

## 3. Estructura del Proyecto

```
Kwitch-cParalela/
├── ANALISIS_KWITCH.md              ← Este archivo
├── README.md                       Documentación principal
├── Documentacion.md                Notas internas de desarrollo
├── LICENSE                         Licencia CC0 1.0
└── kwitch-cparalela/               Módulo Maven
    ├── pom.xml                     Configuración Maven (Java 17)
    └── src/main/java/cpyd/
        ├── StreamServer.java       Servidor de streams (puerto 5000)
        ├── ClientHandler.java      Handler por cliente (streamer o viewer)
        ├── ChatServer.java         Servidor de chat (puerto 6000)
        ├── ChatClientHandler.java  Handler por cliente de chat
        ├── StreamerClient.java     Cliente consola del streamer
        ├── ViewerClient.java       Cliente consola del viewer
        ├── StreamSession.java      DTO del canal/stream
        ├── ChatMessage.java        DTO del mensaje de chat
        ├── ServerResponse.java     Wrapper de respuesta del servidor
        ├── StreamStatus.java       Enum: LIVE, PAUSED, ENDED
        ├── ResponseStatus.java     Enum: OK, ERROR, CHANNEL_CLOSED
        └── Main.java               Placeholder (sin uso activo)
```

---

## 4. Diagrama de Clases

```mermaid
classDiagram
    class Serializable {
        <<interface>>
    }

    class StreamSession {
        +String streamerId
        +String channelName
        +String description
        +String currentApp
        +List~String~ tags
        +StreamStatus status
        +LocalDateTime startTime
        +int viewerCount
        +getters/setters
    }

    class ChatMessage {
        +String user
        +String channelId
        +String content
        +long timestamp
        +toString()
    }

    class ServerResponse {
        +ResponseStatus status
        +String message
        +StreamSession payload
    }

    class StreamStatus {
        <<enumeration>>
        LIVE
        PAUSED
        ENDED
    }

    class ResponseStatus {
        <<enumeration>>
        OK
        ERROR
        CHANNEL_CLOSED
    }

    class Runnable {
        <<interface>>
    }

    class StreamServer {
        +int PORT
        +ConcurrentHashMap~String,StreamSession~ activeSessions
        +ConcurrentHashMap~String,CopyOnWriteArrayList~ClientHandler~~ subscribers
        +main()
    }

    class ClientHandler {
        -Socket socket
        -ObjectInputStream in
        -ObjectOutputStream out
        -String role
        -String channelName
        +run()
        -handleStreamer(StreamSession)
        -handleViewer(String)
        +send(Object)
        -broadcast(String, StreamSession)
        -cleanup()
    }

    class ChatServer {
        -int PORT
        -Map~String,List~ChatClientHandler~~ channels
        -ExecutorService threadPool
        +main()
        +startServer()
        +addClientToChannel()
        +removeClientFromChannel()
        +getChannelClients()
    }

    class ChatClientHandler {
        -Socket socket
        -ChatServer server
        -ObjectOutputStream out
        -ObjectInputStream in
        -String currentChannel
        +run()
        -broadcast(ChatMessage)
        +sendMessage(ChatMessage)
        -cleanUp()
    }

    class StreamerClient {
        -String HOST
        -int PORT
        +main()
    }

    class ViewerClient {
        -String HOST
        -int STREAM_PORT
        -int CHAT_PORT
        -String username
        -String targetChannel
        -Socket chatSocket
        -ObjectOutputStream chatOut
        +start(Scanner)
        -fetchActiveChannels()
        -connectToStreamServer()
        -handleServerNotification(ServerResponse)
        -setupChat()
        -sendChatMessage(String)
        -closeConnections()
        +main()
    }

    StreamSession ..|> Serializable
    ChatMessage ..|> Serializable
    ServerResponse ..|> Serializable
    ClientHandler ..|> Runnable
    ChatClientHandler ..|> Runnable
    StreamSession --> StreamStatus
    ServerResponse --> ResponseStatus
    ServerResponse --> StreamSession
    StreamServer --> ClientHandler : crea
    StreamServer --> StreamSession : gestiona
    StreamServer --> ServerResponse : envía
    ClientHandler --> StreamServer : accede (static)
    ChatServer --> ChatClientHandler : crea
    ChatClientHandler --> ChatServer : accede (instancia)
    StreamerClient --> StreamSession : envía
    StreamerClient --> ServerResponse : recibe
    ViewerClient --> ChatMessage : envía/recibe
    ViewerClient --> ServerResponse : recibe
```

---

## 5. Descripción Detallada de Componentes

### 5.1 DTOs (Data Transfer Objects)

Son objetos serializables que viajan por la red a través de `ObjectInputStream`/`ObjectOutputStream`.

#### `StreamSession` — Representa un canal/stream activo

| Campo | Tipo | Descripción |
|---|---|---|
| `streamerId` | `String` | Identificador del streamer |
| `channelName` | `String` | Nombre del canal (clave única en el mapa) |
| `description` | `String` | Descripción del canal |
| `currentApp` | `String` | Aplicación/juego que se transmite |
| `tags` | `List<String>` | Etiquetas del canal |
| `status` | `StreamStatus` | Estado actual: `LIVE`, `PAUSED` o `ENDED` |
| `startTime` | `LocalDateTime` | Marca temporal de inicio del stream |
| `viewerCount` | `int` | Conteo de espectadores (actualizado por el servidor) |

```java
// StreamerClient.java:66-68
StreamSession session = new StreamSession(
    streamerId, channelName, description, currentApp, tags
);
out.writeObject(session);
```

#### `ChatMessage` — Representa un mensaje de chat

| Campo | Tipo | Descripción |
|---|---|---|
| `user` | `String` | Nombre del remitente |
| `channelId` | `String` | Canal al que pertenece |
| `content` | `String` | Contenido del mensaje |
| `timestamp` | `long` | `System.currentTimeMillis()` al crearse |

```java
// ChatMessage.java:29
return String.format("[%s] %s: %s", channelId, user, content);
```

#### `ServerResponse` — Envoltorio de respuesta del servidor

| Campo | Tipo | Descripción |
|---|---|---|
| `status` | `ResponseStatus` | `OK`, `ERROR` o `CHANNEL_CLOSED` |
| `message` | `String` | Mensaje legible para el cliente |
| `payload` | `StreamSession` | Opcional: estado actualizado del canal (puede ser `null`) |

### 5.2 Enums

#### `StreamStatus` — Ciclo de vida del stream

```mermaid
stateDiagram-v2
    [*] --> LIVE : Streamer abre canal
    LIVE --> PAUSED : Streamer pausa
    PAUSED --> LIVE : Streamer reanuda
    LIVE --> ENDED : Streamer cierra
    PAUSED --> ENDED : Streamer cierra
    ENDED --> [*]
```

#### `ResponseStatus` — Resultado de operaciones del servidor

- **OK** — Operación exitosa
- **ERROR** — Error (ej: canal no encontrado)
- **CHANNEL_CLOSED** — El streamer cerró el canal (broadcast a viewers)

### 5.3 Servidores

#### `StreamServer` — Gestión de canales y notificaciones

- **Puerto:** 5000
- **Modelo:** Thread-per-client (cada `ClientHandler` en un `new Thread()`)
- **Estructuras compartidas** (static, accesibles por todos los handlers):

```java
// Mapa: nombre_canal -> StreamSession
public static final ConcurrentHashMap<String, StreamSession> activeSessions;

// Mapa: nombre_canal -> lista de handlers de viewers suscritos
public static final ConcurrentHashMap<String, CopyOnWriteArrayList<ClientHandler>> subscribers;
```

**Razón de ser `static`:** Todos los `ClientHandler` necesitan acceder a estos mapas para registrar canales, suscribir viewers, hacer broadcast y limpiar al desconectarse. Al ser `static` y usar `ConcurrentHashMap`, se evitan bloqueos explícitos (`synchronized`).

```mermaid
flowchart LR
    subgraph StreamServer["StreamServer (:5000)"]
        direction TB
        AS[(activeSessions\nConcurrentHashMap)]
        SUB[(subscribers\nConcurrentHashMap)]
    end

    S1[Socket 1] --> CH1[ClientHandler A\nhilo]
    S2[Socket 2] --> CH2[ClientHandler B\nhilo]
    S3[Socket 3] --> CH3[ClientHandler C\nhilo]

    CH1 --> AS
    CH1 --> SUB
    CH2 --> AS
    CH2 --> SUB
    CH3 --> AS
    CH3 --> SUB
```

#### `ChatServer` — Mensajería en tiempo real por canal

- **Puerto:** 6000
- **Modelo:** Thread pool (`Executors.newCachedThreadPool()`)
- **Estructura compartida:**

```java
private final Map<String, List<ChatClientHandler>> channels = new ConcurrentHashMap<>();
```

**Ventaja del thread pool** sobre `new Thread()`: Reutiliza hilos inactivos para clientes de chat (operaciones I/O-bound), evitando la sobrecarga de crear un hilo OS por cada conexión.

### 5.4 Manejadores de Clientes

#### `ClientHandler` — Discriminación de rol por primer mensaje

El protocolo de `StreamServer` usa **discriminación por tipo del primer objeto** para determinar el rol del cliente:

```mermaid
flowchart TD
    INICIO[Cliente conecta] --> LEER[Leer primer objeto]
    LEER --> INSTANCEOF{instanceof?}
    INSTANCEOF -->|StreamSession| STREAMER[Rol: STREAMER]
    INSTANCEOF -->|String| STRING{¿String?}
    STRING -->|"LIST_CHANNELS"| LIST[Devolver lista de canales\nCerrar conexión]
    STRING -->|Otro String| VIEWER[Rol: VIEWER]
    STREAMER --> REGISTRAR[Registrar canal en activeSessions]
    REGISTRAR --> ESCUCHAR[Bucle: leer actualizaciones]
    ESCUCHAR --> BROADCAST[Broadcast a viewers suscritos]
    VIEWER --> SUSCRIBIR[Suscribir al canal]
    SUSCRIBIR --> ESPERAR[Bucle: esperar notificaciones]
```

**`handleStreamer()`** — Lógica del streamer:
1. Registra el `StreamSession` en `activeSessions`
2. Inicializa lista de suscriptores vacía en `subscribers`
3. Envía confirmación `ServerResponse(OK)` al streamer
4. Entra en bucle de lectura de actualizaciones de `StreamSession`
5. En cada actualización: ajusta `viewerCount`, actualiza el mapa, hace broadcast a viewers
6. Si el estado es `ENDED`, sale del bucle y el `finally` ejecuta `cleanup()`

**`handleViewer()`** — Lógica del viewer:
1. Verifica que el canal exista en `activeSessions`
2. Si no existe: responde `ServerResponse(ERROR)` y retorna
3. Si existe: agrega `this` (el handler) a `subscribers[canal]`
4. Envía `ServerResponse(OK)` con el estado actual del canal
5. Entra en bucle dormido (`Thread.sleep(3000)`) hasta que el socket se cierre

**`broadcast()`** — Multicast a viewers:

```java
// ClientHandler.java:194-207
for (ClientHandler viewer : viewers) {
    viewer.send(notification);
}
```

Si el estado es `ENDED`, envía `ResponseStatus.CHANNEL_CLOSED`.

**`cleanup()`** — Limpieza garantizada (en `finally`):
- **VIEWER:** lo elimina de `subscribers[channelName]`
- **STREAMER:** elimina el canal de `activeSessions` y `subscribers`
- Cierra el socket

#### `ChatClientHandler` — Distribución de mensajes de chat

1. Lee el primer `ChatMessage` para obtener el `channelId`
2. Se registra en el canal (`server.addClientToChannel()`)
3. Hace broadcast del mensaje de entrada a los demás participantes
4. Entra en bucle: por cada `ChatMessage` recibido, lo retransmite a todos los clientes del mismo canal **excepto el remitente**
5. Timeout de omisión: 60 segundos (`socket.setSoTimeout(60000)`)

### 5.5 Clientes

#### `StreamerClient` — Flujo del streamer

```mermaid
sequenceDiagram
    actor Streamer
    participant SC as StreamerClient
    participant SS as StreamServer

    Streamer->>SC: Ingresa datos del canal
    SC->>SS: StreamSession (LIVE)
    SS-->>SC: ServerResponse(OK)
    Note over SC: Inicia hilo daemon listener
    Note over SC: para recibir viewerCount

    loop Menú de control
        Streamer->>SC: 1. Pausar / 2. Reanudar / 3. Cerrar
        SC->>SS: StreamSession (PAUSED/LIVE/ENDED)
        SS-->>SC: Broadcast a viewers (cambio)
    end
```

**Hilo daemon listener:** Corre en segundo plano escuchando `ServerResponse` del servidor. Cuando un viewer se conecta/desconecta, el servidor actualiza `viewerCount` y lo envía de vuelta al streamer, quien lo imprime en consola.

#### `ViewerClient` — Flujo del viewer

```mermaid
sequenceDiagram
    actor Viewer
    participant VC as ViewerClient
    participant SS as StreamServer
    participant CS as ChatServer

    Viewer->>VC: Ingresa username
    VC->>SS: "LIST_CHANNELS"
    SS-->>VC: Lista de canales activos
    Viewer->>VC: Selecciona canal

    par Conexión StreamServer
        VC->>SS: "nombre_canal"
        SS-->>VC: ServerResponse(OK) + StreamSession
        loop Recibir actualizaciones
            SS-->>VC: ServerResponse (cambio estado)
        end
    and Conexión ChatServer
        VC->>CS: ChatMessage("se ha unido")
        CS-->>VC: ChatMessage (otros usuarios)
        loop Enviar/Recibir mensajes
            Viewer->>VC: Escribe mensaje
            VC->>CS: ChatMessage
            CS-->>VC: ChatMessage (broadcast)
        end
    end
```

**Doble conexión simultánea:** El `ViewerClient` mantiene dos sockets TCP abiertos al mismo tiempo:
1. **StreamServer (:5000)** — para recibir actualizaciones de estado del canal
2. **ChatServer (:6000)** — para enviar y recibir mensajes de chat

El hilo de StreamServer tiene **reconexión automática** con backoff de 5 segundos.

---

## 6. Modelo de Concurrencia

```mermaid
graph TB
    subgraph "Modelo de Concurrencia"
        SS[StreamServer] -->|"new Thread(handler).start()"| T1[ClientHandler #1\nHilo OS]
        SS -->|"new Thread(handler).start()"| T2[ClientHandler #2\nHilo OS]
        SS -->|"new Thread(handler).start()"| T3[ClientHandler #N\nHilo OS]

        CS[ChatServer] -->|"threadPool.execute(handler)"| P1[ChatClientHandler #1\nPool Thread]
        CS -->|"threadPool.execute(handler)"| P2[ChatClientHandler #2\nPool Thread]
        CS -->|"threadPool.execute(handler)"| P3[ChatClientHandler #N\nPool Thread]

        SC[StreamerClient] --> L1[Listener Daemon\nHilo en background]
        VC[ViewerClient] --> L2[Stream Listener\nHilo daemon]
        VC --> L3[Chat Reader\nHilo daemon]
    end
```

| Componente | Estrategia | Justificación |
|---|---|---|
| **StreamServer** | `new Thread(handler).start()` | Cada cliente necesita un hilo dedicado para el bucle de lectura/bloqueo |
| **ChatServer** | `Executors.newCachedThreadPool()` | Clientes I/O-bound: reutiliza hilos, crea nuevos solo si es necesario |
| **StreamerClient** | Daemon thread listener | No debe impedir la terminación del proceso principal |
| **ViewerClient** | 2 daemon threads | Uno para stream, otro para chat; mueren con el main |

### Estructuras Thread-Safe

| Colección | Uso | Propiedad |
|---|---|---|
| `ConcurrentHashMap<String, StreamSession>` | Canales activos | Lectura/escritura concurrente sin bloqueos |
| `ConcurrentHashMap<String, CopyOnWriteArrayList<ClientHandler>>` | Suscriptores por canal | Lecturas frecuentes (broadcast), escrituras ocasionales (suscribir/desuscribir) |
| `CopyOnWriteArrayList<ChatClientHandler>` | Clientes de chat por canal | Iteración segura durante broadcast sin `ConcurrentModificationException` |

---

## 7. Modelo de Fallos

```mermaid
flowchart TD
    FALLO[Fallo detectado] --> TIPO{Tipo de fallo}
    TIPO -->|Crash| CRASH[SocketException / EOFException]
    TIPO -->|Omisión| OMISION[SocketTimeoutException]
    TIPO -->|Lógico| LOGICO[Canal no existe]

    CRASH --> LIMPIEZA[cleanup():\n- Remover de subscribers\n- Remover de activeSessions\n- Cerrar socket]
    OMISION --> LIMPIEZA

    LOGICO --> ERROR[ServerResponse(ERROR)]
    ERROR --> VIEWER[ViewerClient recibe error\nSystem.exit()]
```

| Tipo de fallo | Detección | Mecanismo | Respuesta |
|---|---|---|---|
| **Crash del cliente** | `SocketException`, `EOFException` | Capturadas en `catch` de `run()` | `cleanup()` elimina referencias fantasma |
| **Omisión (cliente silencioso)** | `SocketTimeoutException` | `socket.setSoTimeout(180000)` en stream, `60000` en chat | Limpieza como si fuera crash |
| **Canal inexistente** | `containsKey()` == false | Validación en `handleViewer()` | `ServerResponse(ERROR)` y retorno |
| **Caída del StreamServer** | `IOException` en daemon thread | `connectToStreamServer()` en `ViewerClient` | Reconexión automática cada 5 segundos |
| **Canal cerrado** | Estado `ENDED` | `broadcast()` con `ResponseStatus.CHANNEL_CLOSED` | Viewer notificado; socket se cierra eventualmente |

```java
// Detección de omisión en StreamServer (ClientHandler.java:52)
socket.setSoTimeout(180000); // 3 minutos sin datos → timeout

// Detección de omisión en ChatServer (ChatClientHandler.java:29)
socket.setSoTimeout(60000); // 1 minuto sin datos → timeout

// Reconexión automática en ViewerClient (ViewerClient.java:103-129)
while (true) {
    try (Socket streamSocket = new Socket(HOST, STREAM_PORT)) { ... }
    catch (SocketException | EOFException e) {
        System.err.println("Conexión perdida. Reintentando en 5 segundos...");
    }
    Thread.sleep(5000); // backoff fijo de 5s
}
```

---

## 8. Protocolo de Comunicación

### 8.1 Formato de Mensajes

Todos los mensajes viajan como **objetos Java serializados** sobre TCP:

```
[TCP Header] [Object Header] [serialized data: StreamSession | ChatMessage | ServerResponse | String | List]
```

### 8.2 Secuencia de Operaciones Detallada

#### Apertura de Canal (Streamer → Server)

```mermaid
sequenceDiagram
    participant SC as StreamerClient
    participant CH as ClientHandler (hilo)
    participant SS as StreamServer (datos)

    SC->>CH: Conecta socket
    CH->>SS: activeSessions.put(channel, session)
    CH->>SS: subscribers.putIfAbsent(channel, new CopyOnWriteArrayList<>())
    CH-->>SC: ServerResponse(OK, "Canal abierto:...", session)
    Note over SC,CH: Bucle de actualizaciones
    SC->>CH: StreamSession(PAUSED)
    CH->>SS: activeSessions.put(channel, update)
    CH->>CH: broadcast(channel, update)
    SC->>CH: StreamSession(ENDED)
    CH->>CH: broadcast() con CHANNEL_CLOSED
    CH->>CH: cleanup() → elimina canal
```

#### Suscripción de Viewer y Chat

```mermaid
sequenceDiagram
    participant VC as ViewerClient
    participant CH as ClientHandler (stream)
    participant CCH as ChatClientHandler (chat)

    VC->>CH: "nombre_canal"
    CH->>CH: Verifica activeSessions.containsKey()
    CH-->>VC: ServerResponse(OK, ..., StreamSession)
    CH->>CH: subscribers.get(channel).add(this)

    VC->>CCH: Conecta socket
    CCH->>CCH: Lee ChatMessage → obtiene channelId
    CCH->>CCH: server.addClientToChannel(channelId, this)
    CCH-->>VC: broadcast(ChatMessage("se ha unido"))
```

---

## 9. Compilación y Ejecución

### Requisitos

```bash
java -version   # Java 17 o superior
mvn -version    # Maven 3.6 o superior
```

### Compilación

```bash
cd kwitch-cparalela
mvn compile
```

Los `.class` quedan en `target/classes/`.

### Ejecución (4 terminales)

```bash
# Terminal 1 - StreamServer (puerto 5000)
java -cp target/classes cpyd.StreamServer

# Terminal 2 - ChatServer (puerto 6000)
java -cp target/classes cpyd.ChatServer

# Terminal 3 - StreamerClient
java -cp target/classes cpyd.StreamerClient

# Terminal 4 - ViewerClient
java -cp target/classes cpyd.ViewerClient
```

### Orden de inicio

1. **StreamServer** (primero, los clientes lo necesitan)
2. **ChatServer** (segundo, los viewers lo necesitan)
3. **StreamerClient** (tercero, abre un canal)
4. **ViewerClient** (cuarto, descubre canales y se suscribe)

---

## 10. Resumen Técnico

| Componente | Archivo | Líneas | Rol |
|---|---|---|---|
| StreamServer | `StreamServer.java` | 56 | Servidor TCP :5000, thread-per-client |
| ClientHandler | `ClientHandler.java` | 239 | Lógica de streamer/viewer, broadcast, cleanup |
| ChatServer | `ChatServer.java` | 65 | Servidor TCP :6000, thread pool, canales de chat |
| ChatClientHandler | `ChatClientHandler.java` | 95 | Distribución de mensajes de chat |
| StreamerClient | `StreamerClient.java` | 152 | CLI del streamer, menú de control |
| ViewerClient | `ViewerClient.java` | 207 | CLI del viewer, doble conexión, reconexión |
| StreamSession | `StreamSession.java` | 56 | DTO del canal |
| ChatMessage | `ChatMessage.java` | 31 | DTO del mensaje |
| ServerResponse | `ServerResponse.java` | 46 | DTO de respuesta |
| StreamStatus | `StreamStatus.java` | 13 | Enum LIVE/PAUSED/ENDED |
| ResponseStatus | `ResponseStatus.java` | 13 | Enum OK/ERROR/CHANNEL_CLOSED |
| **Total** | **12 fuentes** | **976** | |

---

## 11. Autores

| Autor | Rol |
|---|---|
| Diego Alvarado Mondaca | DEV 1 |
| Ignacia Brahim Lara | DEV 2 |
| Bárbara Oyarzo Alfaro | DEV 3 |
| Vicente Palma Lucero | DEV 4 |
| Javier Retamal Frez | DEV 5 |
| Ariel Villar San José | DEV 6 |

**Curso:** Computación Paralela y Distribuida

**Licencia:** CC0 1.0 Universal (Dominio Público)
