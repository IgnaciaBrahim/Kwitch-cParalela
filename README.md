# Kwitch
Proyecto universitario que implementa una versión simplificada de una plataforma Twitch-inspired, desarrollado en Java con enfoque en sistemas distribuidos de bajo nivel incorporando frameworks como Spark.

`Funcionalidad 1: Stream - Servidor - Usuario`

Cada Streamer puede abrir su transmisión/canal (StreamSession), que luego va a viajar del cliente al servidor. La respuesta del servidor (ServerResponse) viaja del servidor al cliente, y las dos clases de definición de estado de las operaciones (Stream/Response Status) son los mensajes que usan ambos para entenderse.
