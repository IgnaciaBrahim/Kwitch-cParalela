package cpyd;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.Scanner;

/**
 * Cliente para los espectadores de Kwitch.
 * Se conecta al StreamServer para recibir estados y al ChatServer para interactuar.
 */
public class ViewerClient {

    private static final String HOST = "127.0.0.1";
    private static final int STREAM_PORT = 5000;
    private static final int CHAT_PORT = 6000;

    private final String username;
    private String targetChannel;
    
    // Conexión persistente para el chat
    private Socket chatSocket;
    private ObjectOutputStream chatOut;

    public ViewerClient(String username) {
        this.username = username;
    }

    public void start(Scanner scanner) {
        try {
            System.out.println("\n--- BIENVENIDO A KWITCH ---");
            System.out.print("Nombre del canal al que desea unirse: ");
            this.targetChannel = scanner.nextLine();

            // 1. Iniciamos la escucha del StreamServer en un hilo separado
            Thread streamListener = new Thread(this::connectToStreamServer);
            streamListener.setDaemon(true);
            streamListener.start();

            // 2. Iniciamos la conexión al ChatServer
            setupChat();

            // 3. Bucle principal para el envío de mensajes de chat
            System.out.println("\nSistema listo. Escriba su mensaje y presione Enter.");
            System.out.println("(Escriba 'SALIR' para cerrar la aplicación)\n");

            while (true) {
                String text = scanner.nextLine();

                if ("SALIR".equalsIgnoreCase(text)) {
                    break;
                }

                if (chatOut != null && !text.isEmpty()) {
                    sendChatMessage(text);
                }
            }

        } catch (Exception e) {
            System.err.println("Error en la ejecución del cliente: " + e.getMessage());
        } finally {
            closeConnections();
        }
    }

    private void connectToStreamServer() {
        // Lógica de reintentos en caso de que el servidor no esté activo aún
        while (true) {
            try (Socket streamSocket = new Socket(HOST, STREAM_PORT)) {
                ObjectOutputStream out = new ObjectOutputStream(streamSocket.getOutputStream());
                out.flush();
                ObjectInputStream in = new ObjectInputStream(streamSocket.getInputStream());

                // Según el ClientHandler, el primer mensaje debe ser un String con el nombre del canal
                out.writeObject(targetChannel);
                out.flush();

                while (!streamSocket.isClosed()) {
                    Object response = in.readObject();

                    if (response instanceof ServerResponse res) {
                        handleServerNotification(res);
                    }
                }

            } catch (SocketException | java.io.EOFException e) {
                System.err.println("\n[StreamServer] Conexión perdida. Reintentando en 5 segundos...");
            } catch (IOException | ClassNotFoundException e) {
                System.err.println("\n[StreamServer] Error de red: " + e.getMessage());
            }

            try { Thread.sleep(5000); } catch (InterruptedException ignored) {}
        }
    }

    private void handleServerNotification(ServerResponse res) {
        // Si hay un error (ej: canal no existe), lo mostramos y cerramos
        if (res.getStatus() == ResponseStatus.ERROR) {
            System.err.println("\n[ERROR] " + res.getMessage());
            System.exit(0);
        }

        // Si el estado es CHANNEL_CLOSED, informamos al usuario
        if (res.getStatus() == ResponseStatus.CHANNEL_CLOSED) {
            System.out.println("\n[INFO] El streamer ha finalizado la transmisión.");
        }

        // Actualización de datos del stream (vistas, estado, etc.)
        if (res.getPayload() != null) {
            StreamSession session = res.getPayload();
            System.out.println("\n>>> UPDATE: Canal '" + session.getChannelName() + 
                               "' | Estado: " + session.getStatus() + 
                               " | Vistas: " + session.getViewerCount());
        }
    }

    private void setupChat() {
        try {
            chatSocket = new Socket(HOST, CHAT_PORT);
            chatOut = new ObjectOutputStream(chatSocket.getOutputStream());
            chatOut.flush();
            ObjectInputStream chatIn = new ObjectInputStream(chatSocket.getInputStream());

            // Mensaje inicial para registrarse en el canal del chat
            chatOut.writeObject(new ChatMessage(username, targetChannel, "se ha unido al chat."));
            chatOut.flush();

            // Hilo para recibir mensajes de otros usuarios (Broadcast)
            Thread chatReader = new Thread(() -> {
                try {
                    while (true) {
                        Object obj = chatIn.readObject();
                        if (obj instanceof ChatMessage msg) {
                            // Imprimimos el mensaje formateado
                            System.out.println(msg.toString());
                        }
                    }
                } catch (Exception e) {
                    System.err.println("[Chat] Desconectado del servidor de chat.");
                }
            });
            chatReader.setDaemon(true);
            chatReader.start();

        } catch (IOException e) {
            System.err.println("[Chat] No se pudo conectar al servidor de chat.");
        }
    }

    private void sendChatMessage(String content) {
        try {
            ChatMessage msg = new ChatMessage(username, targetChannel, content);
            chatOut.writeObject(msg);
            chatOut.flush();
            chatOut.reset(); // Importante para no enviar objetos cacheados
        } catch (IOException e) {
            System.err.println("[Chat] Error al enviar mensaje.");
        }
    }

    private void closeConnections() {
        try {
            if (chatOut != null) chatOut.close();
            if (chatSocket != null) chatSocket.close();
            System.out.println("Conexiones cerradas correctamente.");
        } catch (IOException ignored) {}
    }

    public static void main(String[] args) {
        // try-with-resources asegura el cierre del Scanner y de System.in al finalizar el programa
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.print("Ingrese su nombre de usuario: ");
            String name = scanner.nextLine();
            
            new ViewerClient(name).start(scanner);
        }
    }
}