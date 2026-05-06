# Kwitch
Proyecto universitario que implementa una versión simplificada de una plataforma Twitch-inspired, desarrollado en Java con enfoque en sistemas distribuidos de bajo nivel incorporando frameworks como Spark.

`Funcionalidad 1: Stream - Servidor - Usuario`

Cada Streamer puede abrir su transmisión/canal (StreamSession), que luego va a viajar del cliente al servidor. La respuesta del servidor (ServerResponse) viaja del servidor al cliente, y las dos clases de definición de estado de las operaciones (Stream/Response Status) son los mensajes que usan ambos para entenderse.

## --> Organización:
### La base:

- Modelo físico (diagrama) (Hecho)
- Diagrama de secuencia — Función 1 (Hecho)
- Clases base: StreamSession, ServerResponse, StreamStatus, ResponseStatus (Hecho)

### Implementación Función 1

`Clases`
- StreamServer.java — ServerSocket, ciclo de aceptación, Thread-per-client (Hecho)
- ClientHandler.java — hilo dedicado por cliente, deserialización, lógica por tipo de cliente (Hecho)
- StreamerClient.java — consola del streamer, envío de sesión, cambio de estados (Pend)
- ViewerClient.java — suscripción al canal, recepción de notificaciones (Pend)

`Informe`
- 1.1 Fundamentación y Teoría: concurrencia, fallos, transparencia (con ejemplos del código) (Pend)
- 1.2 Modelado de Ingeniería: modelo físico, diagrama de secuencia (Hecho) 
  Modelo arquitectónico (Pend), diagrama de secuencia F2 (NA)
- 1.3 Análisis Fundamental: modelo de seguridad, modelo de fallos (crash y omisión) (Pend)
