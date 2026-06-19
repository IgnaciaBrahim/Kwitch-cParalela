package cpyd.handler;

import cpyd.distributed.MessageWithClock;
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

/*maneja la conexion de un viewer en el chat.

Ahora usa MessageWithClock para que los mensajes entre handlers tengan
timestamp de Lamport. Asi se puede ordenar causalmente.

Reglas de Lamport:
- LC1: antes de enviar, se hace clock.tick() y se adjunta el valor
- LC2: al recibir, se hace clock.update(timestampDelOtro)

El viewer sigue enviando/recibiendo ChatMessage normal, el wrapper
MessageWithClock solo se usa internamente entre handlers.
*/

public class ChatClientHandler implements Runnable {
    private final Socket socket;
    private final ChatServer server;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private String currentChannel;

    public ChatClientHandler(Socket socket, ChatServer server) {
        this.socket = socket;
        this.server = server;
    }

    @Override
    public void run() {
        try {
            //timeout de 60s para detectar omision
            socket.setSoTimeout(60000);

            out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            in = new ObjectInputStream(socket.getInputStream());

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

    /*redistribuye el mensaje a los demas participantes del canal
    pero antes lo envuelve en MessageWithClock con timestamp Lamport.
    */
    private void broadcast(ChatMessage message) {
        //LC1: incrementar reloj antes de enviar
        int ts = server.getClock().tick();
        MessageWithClock wrapped = new MessageWithClock(ts, server.getNodeId(), "CHAT", message);

        //log del evento con marca Lamport
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
    Aplica LC2: actualiza el reloj con el timestamp del que llega.
    Despues extrae el ChatMessage y lo envia al viewer.
    */
    public void sendMessage(MessageWithClock msg) {
        try {
            //LC2: al recibir, actualizar reloj
            server.getClock().update(msg.getLamportTime());

            ChatMessage chatMessage = (ChatMessage) msg.getPayload();
            out.writeObject(chatMessage);
            out.flush();
        } catch (IOException e) {
            System.err.println("Fallo al enviar mensaje al viewer. El cliente remoto probablemente se desconecto.");
        }
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