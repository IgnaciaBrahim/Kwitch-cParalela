package cpyd;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Scanner;

public class ViewerClient {
    private static final String SERVER_HOST = "127.0.0.1";
    private static final int STREAM_SERVER_PORT = 5000;
    private static final int CHAT_SERVER_PORT = 6000;
    
    private final String username;
    private String currentChannel;
    private ObjectOutputStream chatOut;
    private Socket chatSocket;

    public ViewerClient(String username) {
        this.username = username;
    }

    public void start(Scanner scanner) {
        System.out.println("=== Bienvenido a Kwitch ===");
        System.out.print("Ingrese el nombre del canal al que desea unirse: ");
        this.currentChannel = scanner.nextLine();

        // Se usa un hilo independiente para escuchar eventos del Stream Server (Ej: Inicio/Pausa/Fin)
        // Esto evita que la lectura bloqueante de red detenga la interfaz.
        Thread streamThread = new Thread(this::listenToStreamServer);
        streamThread.start();

        // Se inicializa la conexión al Chat Server y se levanta otro hilo para escuchar mensajes entrantes.
        setupChatConnection();

        // El hilo principal (Main) queda dedicado exclusivamente a capturar el input del usuario en consola.
        System.out.println("Conectado. Puede comenzar a chatear (escriba 'SALIR' para desconectar):");
        while (true) {
            String input = scanner.nextLine();
            if ("SALIR".equalsIgnoreCase(input)) {
                System.out.println("Cerrando sesión de Kwitch...");
                System.exit(0); // Esto cierra la app
            }
            
            if (chatOut != null && !input.trim().isEmpty()) {
                sendMessage(input);
            }
        }
    }

    private void listenToStreamServer() {
        // Retry logic: Si el servidor de streaming cae, intentamos reconectar continuamente.
        while (true) {
            try (Socket socket = new Socket(SERVER_HOST, STREAM_SERVER_PORT)) {
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                // MODIFICAR DESPUES DE QUE LA CLASE STREAMSERVER ESTE COMPLETA
                out.writeObject("SUBSCRIBE:" + currentChannel);
                out.flush();

                while (true) {
                    Object obj = in.readObject();
                    if (obj instanceof StreamSession) {
                        StreamSession session = (StreamSession) obj;
                        System.out.println("\n[STREAM UPDATE] Estado: " + session.getStatus());
                    } else if (obj instanceof ServerResponse) {
                        ServerResponse response = (ServerResponse) obj;
                        System.out.println("\n[SERVER] " + response.getMessage());
                        if ("CHANNEL_CLOSED".equals(response.getMessage())) {
                            System.out.println("\nTransmisión finalizada por el streamer.");
                            System.exit(0);
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("\n[Alerta] Buscando Stream Server en puerto " + STREAM_SERVER_PORT + "... (Reintento en 3s)");
                try { Thread.sleep(3000); } catch (InterruptedException ignored) {}
            }
        }
    }

    private void setupChatConnection() {
        // Hilo de lectura continua para el chat
        Thread chatListener = new Thread(() -> {
            try {
                chatSocket = new Socket(SERVER_HOST, CHAT_SERVER_PORT);
                chatOut = new ObjectOutputStream(chatSocket.getOutputStream());
                chatOut.flush();
                ObjectInputStream chatIn = new ObjectInputStream(chatSocket.getInputStream());

                // Enviamos un primer mensaje automático para que el ChatServer nos registre en la lista del canal
                ChatMessage joinMessage = new ChatMessage(username, currentChannel, "se ha unido al chat.");
                chatOut.writeObject(joinMessage);
                chatOut.flush();

                while (true) {
                    Object obj = chatIn.readObject();
                    if (obj instanceof ChatMessage) {
                        ChatMessage msg = (ChatMessage) obj;
                        // Hilo de lectura continua para el chat
                        System.out.println("\r" + msg.toString());
                    }
                }
            } catch (Exception e) {
                System.err.println("\n[Alerta] Desconectado del Chat Server.");
            } finally {
                // Limpieza forzosa de la conexión al terminar el hilo
                try {
                    if (chatOut != null) chatOut.close();
                    if (chatSocket != null && !chatSocket.isClosed()) chatSocket.close();
                } catch (Exception ignored) {}
            }
        });
        chatListener.start();
    }

    private void sendMessage(String content) {
        try {
            ChatMessage message = new ChatMessage(username, currentChannel, content);
            chatOut.writeObject(message);
            chatOut.flush();
        } catch (Exception e) {
            System.err.println("Error de red al enviar el mensaje.");
        }
    }

    public static void main(String[] args) {
    // try-with-resources cierra el scanner automáticamente al finalizar
    try (Scanner scanner = new Scanner(System.in)) {
        System.out.print("Ingrese su nombre de usuario: ");
        String user = scanner.nextLine();
        
        new ViewerClient(user).start(scanner);
    }
}
}