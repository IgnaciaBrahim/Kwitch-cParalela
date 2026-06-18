# Checklist vs Proyecto Final ICI4344.md

---

## Modificaciones Javier hasta ahora

**Reorganización en subpaquetes (12 archivos movidos):**
```
cpyd/
├── model/     → StreamSession, ChatMessage, ServerResponse, StreamStatus, ResponseStatus
├── server/    → StreamServer, ChatServer
├── handler/   → ClientHandler, ChatClientHandler
├── client/    → StreamerClient, ViewerClient
├── distributed/  
└── loadtest/      
```

**Archivos nuevos creados — puntos de la checklist que cumplen:**

| Archivo | Checklist que avanza |
|---|---|
| `distributed/LamportClock.java` | **2.2** Ordenamiento de eventos — reloj lógico Lamport |
| `distributed/MessageWithClock.java` | **2.2** Ordenamiento de eventos — wrapper con timestamp para mensajes entre nodos |
| `distributed/NodeMembership.java` | **2.1** Topología multinodo — registro y estado de nodos (ALIVE/FAILED) |
| `loadtest/MetricsCollector.java` | **3.2** Métricas — throughput, latencia avg/p95, error rate, mensajes coordinación |

---

## 2.1 Topología multinodo

**Cumple con:**
- 4 procesos independientes con `main()`: `server/StreamServer.java`, `server/ChatServer.java`, `client/StreamerClient.java`, `client/ViewerClient.java`
- Sockets TCP + serialización de objetos: `ObjectOutputStream`/`ObjectInputStream` en todos los handlers
- Servidor multihilo: `new Thread(handler)` en `StreamServer.java:49`, `threadPool.execute(handler)` en `ChatServer.java:40`

**Falta:**
- Solo 2 servidores (la rúbrica pide 3+ nodos). No existe `CoordinatorNode`.
- No hay comunicación entre servidores (StreamServer y ChatServer no se conocen).
- No hay membresía/descubrimiento de nodos (no existe `NodeMembership`).

---

## 2.2 Ordenamiento de eventos

**Cumple con:**
- Nada.

**Falta:**
- No existe `LamportClock` ni reloj vectorial.
- No hay orden causal en ninguna función.
- No hay log con marcas lógicas. `ChatMessage.java:19` usa `System.currentTimeMillis()` (reloj físico).

---

## 2.3 Coordinación distribuida

**Cumple con:**
- Nada.

**Falta:**
- No existe `RicartAgrawala` ni Bully ni ningún algoritmo de coordinación.
- No hay recurso crítico protegido con exclusión mutua distribuida.
- No hay elección de coordinador.

---

## 2.4 Tolerancia a fallos

**Cumple con:**
- Timeout detección omisión: `socket.setSoTimeout(180000)` en `ClientHandler.java:57`, `setSoTimeout(60000)` en `ChatClientHandler.java:32`
- Detección crash: catch `SocketException | EOFException` en `ClientHandler.java:86`, `ChatClientHandler.java:55`
- Cleanup en finally: `cleanup()` en `ClientHandler.java:216`, `cleanUp()` en `ChatClientHandler.java:86`
- Reconexión viewer: `while(true)` con sleep 5s en `ViewerClient.java:108-135`

**Falta:**
- No hay heartbeats entre nodos (no existe `HeartbeatMonitor`).
- No hay recuperación (re-elección, redistribución, reintegración).
- No hay detección de caída de servidores, solo de clientes.

---

## 3.1 Generador de carga

**Cumple con:**
- Nada.

**Falta:**
- No existe `LoadGenerator` con 50+ hilos por 60+ segundos.
- No ejercita las 2 funciones principales ni el recurso protegido.

---

## 3.2 Métricas

**Cumple con:**
- Nada.

**Falta:**
- No existe `MetricsCollector`.
- No hay medición de throughput, latencia avg/p95, mensajes de coordinación, ni tasa de error.

---

## 3.3 Falla inducida

**Cumple con:**
- Nada.

**Falta:**
- No hay mecanismo para matar un nodo durante la prueba.
- No hay medición de tiempo de recuperación.

---

## 4.4 Distribución y Comunicación

**Cumple con:**
- 4 procesos con sockets TCP: `StreamServer.java`, `ChatServer.java`, `StreamerClient.java`, `ViewerClient.java`
- Marshalling con serialización: `ChatMessage`, `ServerResponse`, `StreamSession` implementan `Serializable`
- Estructuras thread-safe: `ConcurrentHashMap` en `StreamServer.java:31-36` y `ChatServer.java:21`, `CopyOnWriteArrayList` en `StreamServer.java:35`

**Falta:**
- Solo 2 nodos servidores (no 3+).
- No hay membresía/descubrimiento entre nodos.
- No hay comunicación inter-servidor.

---

## 4.5 Coordinación y Ordenamiento

**Cumple con:**
- Concurrencia thread-safe: `ConcurrentHashMap`, `CopyOnWriteArrayList`, `Executors.newCachedThreadPool` — todo sin `synchronized` explícito.

**Falta:**
- No hay reloj lógico (Lamport/vectorial).
- No hay algoritmo de coordinación (Ricart-Agrawala/Bully).
- No hay ordenamiento verificable.

---

## 4.6 Tolerancia a Fallos y Funciones

**Cumple con:**
- Función 1 (streaming): `StreamerClient` crea/pausa/reanuda/cierra canales; `StreamServer` + `ClientHandler` maneja sesiones y broadcast. `StreamerClient.java:66-145`, `ClientHandler.java:100-136`
- Función 2 (chat): `ViewerClient` envía/recibe mensajes; `ChatServer` + `ChatClientHandler` distribuye por canal. `ViewerClient.java:150-189`, `ChatClientHandler.java:28-61`
- Manejo excepciones de red en todos los handlers.
- Detección de caída de clientes vía SocketException/EOFException.

**Falta:**
- No hay detección de caída entre servidores (solo clientes).
- No hay recuperación (re-elección, reconfiguración, reintegración).
- No probado bajo carga (no existe LoadGenerator).

---

## Resumen

| Requisito | Estado |
|---|---|
| **2.1** Topología multinodo | ⚠️ Parcial (solo 2 servidores, sin membresía) |
| **2.2** Ordenamiento eventos | ❌ No existe |
| **2.3** Coordinación distribuida | ❌ No existe |
| **2.4** Tolerancia a fallos | ⚠️ Parcial (solo detección cliente) |
| **3.1** Generador de carga | ❌ No existe |
| **3.2** Métricas | ❌ No existe |
| **3.3** Falla inducida | ❌ No existe |
| **4.4** Distribución | ⚠️ Parcial (sockets bien, faltan nodos y membresía) |
| **4.5** Coordinación y Ordenamiento | ⚠️ Parcial (concurrencia bien, no hay reloj ni algoritmo) |
| **4.6** Tolerancia a Fallos | ⚠️ Parcial (funciones andan, detección parcial, sin recuperación) |
