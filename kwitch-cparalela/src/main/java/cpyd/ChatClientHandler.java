package cpyd;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.List;

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
            // Detección de clientes silenciosos (fallos tipo omisión)
            // Lanza SocketTimeoutException si el socket no recibe lecturas en 60 segundos
            socket.setSoTimeout(60000);

            out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            in = new ObjectInputStream(socket.getInputStream());

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
            
        // Manejo de excepciones de red explícito
        } catch (SocketTimeoutException e) {
            System.err.println("Desconexión por timeout (Omisión). El cliente no envió mensajes a tiempo.");
        } catch (SocketException | EOFException e) {
            System.err.println("Desconexión detectada (Crash) del cliente en canal " + currentChannel + ": " + e.getMessage());
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Fallo interno de I/O o estructura de clase inválida: " + e.getMessage());
        } finally {
            cleanUp();
        }
    }

    // Redistribución de mensajes a todos los participantes del canal
    private void broadcast(ChatMessage message) {
        List<ChatClientHandler> clients = server.getChannelClients(message.getChannelId());
        if (clients != null) {
            for (ChatClientHandler client : clients) {
                // Validación para no reenviar el mensaje al emisor original
                if (!this.equals(client)) {
                    client.sendMessage(message);
                }
            }
        }
    }

    public void sendMessage(ChatMessage message) {
        try {
            out.writeObject(message);
            out.flush(); 
        } catch (IOException e) {
            System.err.println("Fallo al enviar mensaje. El cliente remoto probablemente se desconectó.");
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