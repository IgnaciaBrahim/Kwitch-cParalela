package cpyd.handler;

import cpyd.distributed.MessageWithClock;
import cpyd.distributed.SafeObjectInputStream;
import cpyd.model.ChatMessage;
import cpyd.server.ChatServer;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

/*maneja la conexion de un viewer en el chat.

Usa MessageWithClock para que los mensajes entre handlers tengan
timestamp de Lamport. Los mensajes recibidos se ordenan en una cola
de prioridad por (lamportTime, senderId) antes de entregarse al viewer,
asi se garantiza orden causal.

Reglas de Lamport:
- LC1: antes de enviar, se hace clock.tick() y se adjunta el valor
- LC2: al recibir, se hace clock.update(timestampDelOtro)
*/

public class ChatClientHandler implements Runnable {
    private final Socket socket;
    private final ChatServer server;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private String currentChannel;

    //cola que ordena mensajes por timestamp Lamport (y senderId para desempate)
    private final PriorityBlockingQueue<MessageWithClock> deliveryQueue =
        new PriorityBlockingQueue<>(11, (a, b) -> {
            int cmp = Integer.compare(a.getLamportTime(), b.getLamportTime());
            if (cmp != 0) return cmp;
            return a.getSenderId().compareTo(b.getSenderId());
        });

    public ChatClientHandler(Socket socket, ChatServer server) {
        this.socket = socket;
        this.server = server;
    }

    @Override
    public void run() {
        try {
            socket.setSoTimeout(60000);

            out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            in = new SafeObjectInputStream(socket.getInputStream());

            //hilo que entrega mensajes al viewer en orden Lamport
            Thread deliveryThread = new Thread(this::deliveryLoop, "chat-delivery-" + socket.getPort());
            deliveryThread.setDaemon(true);
            deliveryThread.start();

            //el primer mensaje identifica el canal
            Object obj = in.readObject();
            if (obj instanceof ChatMessage initialMsg) {
                currentChannel = initialMsg.getChannelId();
                server.addClientToChannel(currentChannel, this);
                broadcast(initialMsg);
            }

            while (true) {
                Object msgObj = in.readObject();
                if (msgObj instanceof ChatMessage chatMessage) {
                    broadcast(chatMessage);
                }
            }

        } catch (SocketTimeoutException e) {
            System.err.println("Desconexion por timeout (Omision). El cliente no envio mensajes a tiempo.");
        } catch (SocketException | EOFException e) {
            System.err.println("Desconexion detectada (Crash) del cliente en canal " + currentChannel + ": " + e.getMessage());
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Fallo interno de I/O o estructura de clase invalida: " + e.getMessage());
        } finally {
            cleanUp();
        }
    }

    /*hilo que lee de la cola ordenada y escribe al viewer.
    Los mensajes se entregan en orden ascendente de Lamport.

    Usa poll() con timeout en vez de take(): si el canal no tiene otros
    participantes, este handler nunca recibe un broadcast y take() se
    quedaria bloqueado para siempre, incluso despues de que el cliente
    se desconecte. Eso filtraba un hilo nativo por cada conexion de chat
    que no recibia mensajes, hasta agotar los hilos del sistema operativo
    bajo carga sostenida. Con poll() el hilo revisa cada 2s si el socket
    ya se cerro y termina solo.*/
    private void deliveryLoop() {
        try {
            while (!socket.isClosed()) {
                MessageWithClock msg = deliveryQueue.poll(2, TimeUnit.SECONDS);
                if (msg == null) continue; //nada que entregar, reintentar
                ChatMessage chatMessage = (ChatMessage) msg.getPayload();
                out.writeObject(chatMessage);
                out.flush();
            }
        } catch (Exception e) {
            //el socket se cerro, el hilo termina
        }
    }

    /*redistribuye el mensaje a los demas participantes del canal
    envuelto en MessageWithClock con timestamp Lamport (LC1).*/
    private void broadcast(ChatMessage message) {
        int ts = server.getClock().tick();
        MessageWithClock wrapped = new MessageWithClock(ts, server.getNodeId(), "CHAT", message);

        server.getLogger().logLamport(ts, "[CHAT] " + message.getChannelId()
            + " <" + message.getUser() + ">: " + message.getContent());

        List<ChatClientHandler> clients = server.getChannelClients(message.getChannelId());
        if (clients != null) {
            for (ChatClientHandler client : clients) {
                if (!this.equals(client)) {
                    client.sendMessage(wrapped);
                }
            }
        }
    }

    /*recibe un MessageWithClock de otro handler.
    Aplica LC2 y lo encola para entrega ordenada.*/
    public void sendMessage(MessageWithClock msg) {
        server.getClock().update(msg.getLamportTime());
        deliveryQueue.add(msg);
    }

    private void cleanUp() {
        if (currentChannel != null) {
            server.removeClientFromChannel(currentChannel, this);
        }
        try {
            if (out != null) out.close();
            if (in != null) in.close();
            if (socket != null && !socket.isClosed()) socket.close();
        } catch (IOException e) {
            System.err.println("Error al liberar recursos del socket en cleanUp: " + e.getMessage());
        }
    }
}
