# Kwitch

Plataforma de streaming distribuida inspirada en Twitch, desarrollada en Java puro con sockets TCP. Proyecto universitario para el ramo ICI-4344 Computación Paralela y Distribuida.

## Funciones principales

**1. Streaming en vivo** — Un streamer abre un canal y transmite su estado (LIVE / PAUSED / ENDED). Los espectadores se suscriben y reciben actualizaciones en tiempo real mediante broadcast.

**2. Chat en vivo** — Sistema de chat multicanal donde streamers y espectadores pueden enviarse mensajes en tiempo real dentro del mismo canal.

## Arquitectura

Dos servidores independientes corren en paralelo:

| Componente | Puerto | Rol |
|---|---|---|
| `StreamServer` | 5000 | Gestiona canales activos y notifica a suscriptores |
| `ChatServer` | 6000 | Distribuye mensajes de chat por canal |
| `StreamerClient` | — | Controla el stream y participa en el chat |
| `ViewerClient` | — | Se suscribe al stream y participa en el chat |


## Cómo ejecutar

Abrir 5 terminales en la raíz del proyecto y ejecutar en orden:

**1. StreamServer**
```
java -cp kwitch-cparalela\target\classes cpyd.StreamServer
```

**2. ChatServer**
```
java -cp kwitch-cparalela\target\classes cpyd.ChatServer
```

**3. StreamerClient**
```
java -cp kwitch-cparalela\target\classes cpyd.StreamerClient
```

**4 y 5. ViewerClient** (una terminal por espectador)
```
java -cp kwitch-cparalela\target\classes cpyd.ViewerClient
```

> Compilar antes con Maven: `mvn compile` dentro de `kwitch-cparalela/`
