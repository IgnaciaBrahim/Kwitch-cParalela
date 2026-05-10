// StreamerClient.java
package cpyd;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Scanner;

public class StreamerClient {

    private static final String HOST = "localhost";
    private static final int PORT = 5000;

    public static void main(String[] args) throws Exception{
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("=== Kwitch — Streamer Console ===");
            System.out.print("Tu ID de streamer: ");
            String streamerId = scanner.nextLine();

            System.out.print("Nombre del canal: ");
            String channelName = scanner.nextLine();

            System.out.print("Descripción: ");
            String description = scanner.nextLine();

            System.out.print("App que estás mostrando: ");
            String currentApp = scanner.nextLine();

            System.out.print("Tags (separados por coma): ");
            String[] tagsArray = scanner.nextLine().split(",");
            var tags = Arrays.asList(tagsArray);

            try (Socket socket = new Socket(HOST, PORT)) {
                // OOS antes que OIS
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream  in  = new ObjectInputStream(socket.getInputStream());

                // Crear y enviar sesión inicial
                StreamSession session = new StreamSession(
                    streamerId, channelName, description, currentApp, tags
                );
                out.writeObject(session);
                out.flush();

                // Leer confirmación del servidor
                ServerResponse response = (ServerResponse) in.readObject();
                System.out.println("\n[Servidor] " + response.getMessage());
                System.out.println("[Servidor] Estado: " + response.getStatus());

                // Hilo separado para recibir actualizaciones del servidor (viewerCount)
                Thread listener = new Thread(() -> {
                    try {
                        while (true) {
                            ServerResponse update = (ServerResponse) in.readObject();
                            StreamSession payload = update.getPayload();
                            if (payload != null) {
                                System.out.println("\n[Actualización] Espectadores: "
                                    + payload.getViewerCount());
                            }
                        }
                    } catch (EOFException | SocketException e) {
                        System.out.println("[Streamer] Conexión cerrada.");
                    } catch (IOException | ClassNotFoundException e) {
                        System.err.println("[Streamer] Error en listener: " + e.getMessage());
                    }
                });
                listener.setDaemon(true);
                listener.start();

                // Menú principal
                boolean running = true;
                while (running) {
                    System.out.println("\n--- Opciones ---");
                    System.out.println("1. Pausar stream");
                    System.out.println("2. Reanudar stream");
                    System.out.println("3. Cerrar canal");
                    System.out.print("Elige: ");

                    String option = scanner.nextLine();

                    switch (option) {
                        case "1" -> {
                            session.setStatus(StreamStatus.PAUSED);
                            out.writeObject(session);
                            out.flush();
                            out.reset();
                            System.out.println("[Streamer] Stream pausado.");
                        }
                        case "2" -> {
                            session.setStatus(StreamStatus.LIVE);
                            out.writeObject(session);
                            out.flush();
                            out.reset();
                            System.out.println("[Streamer] Stream reanudado.");
                        }
                        case "3" -> {
                            session.setStatus(StreamStatus.ENDED);
                            out.writeObject(session);
                            out.flush();
                            out.reset();
                            System.out.println("[Streamer] Canal cerrado.");
                            running = false;
                        }
                        default -> System.out.println("Opción no válida.");
                    }
                }

            } catch (IOException | ClassNotFoundException e) {
                System.err.println("[Streamer] Error de conexión: " + e.getMessage());
            }
        }
    }
}
