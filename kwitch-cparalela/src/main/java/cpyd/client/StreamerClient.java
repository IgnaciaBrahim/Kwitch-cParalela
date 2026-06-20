package cpyd.client;
import cpyd.distributed.SafeObjectInputStream;
import cpyd.model.ChatMessage;
import cpyd.model.ServerResponse;
import cpyd.model.StreamSession;
import cpyd.model.StreamStatus;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Scanner;


/*el StreamerClient es la interfaz del streamer. Es el proceso que representa al usuario que va a 
transmitir en Kwitch.

Primero establece el canal: recoge los datos del stream por consola (nombre del canal, descripción,
app, tags), crea un objeto StreamSession y lo envía al StreamServer para registrar el canal como 
activo.

Luego gestiona el estado del canal: presenta un menú en la consola con las opciones de pausar, 
reanudar y cerrar. Cada vez que el streamer elige una opción, actualiza el estado de la StreamSession
y la envía al servidor, que se encarga de notificar a todos los espectadores suscritos.

También puede recibir actualizaciones del servidor: corre un hilo listener en paralelo que escucha 
notificaciones del servidor, principalmente el viewerCount actualizado cada vez que un espectador se
conecta o desconecta. Así el streamer puede ver en tiempo real cuántas personas están viendo su canal
sin interrumpir el menú de control. 

*/

public class StreamerClient {

    private static final String HOST = "localhost";
    private static final int PORT = 5000;

    public static void main(String[] args) throws Exception{

        try (Scanner scanner = new Scanner(System.in)) {
            //esto hace el setup inicial del canal
            System.out.println("=== Kwitch Streamer: Vista Consola ===");
            System.out.print("Tu ID de streamer: ");
            String streamerId = scanner.nextLine();

            System.out.print("Nombre del canal: ");
            String channelName = scanner.nextLine();

            System.out.print("Descripción: ");
            String description = scanner.nextLine();

            System.out.print("App que estás mostrando (ej: League of Legends): ");
            String currentApp = scanner.nextLine();

            System.out.print("Tags (separados por coma, como 'ARAM': ");
            //tag separados
            String[] tagsArray = scanner.nextLine().split(","); 
            var tags = Arrays.asList(tagsArray);

            try (Socket socket = new Socket(HOST, PORT)) {
                //out antes que el in (no deadlock)
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream  in  = new SafeObjectInputStream(socket.getInputStream());

                // Crear y enviar sesión inicial
                StreamSession session = new StreamSession(streamerId, channelName, description, 
                                                                            currentApp, tags
                );
                out.writeObject(session);
                //header
                out.flush();

                //lee la confirmacion del server, el response.
                ServerResponse response = (ServerResponse) in.readObject();
                System.out.println("\n[Servidor] " + response.getMessage());
                System.out.println("[Servidor] Estado: " + response.getStatus());

                //abre un thread para escuchar las notificaciones del server 
                Thread listener = new Thread(() -> {
                    try {
                        while (true) {
                            ServerResponse update = (ServerResponse) in.readObject();
                            StreamSession payload = update.getPayload();
                            //si hay payload entonces hay cambios (!=null)
                            if (payload != null) {
                                System.out.println("\n[Actualización] Espectadores: "
                                    + payload.getViewerCount());
                            }
                        }
                    } catch (SocketException | EOFException e) {
                        System.out.println("[Streamer] Conexión cerrada.");
                        Thread.currentThread().interrupt(); //se comunica la interrupcion
                    } catch (IOException | ClassNotFoundException e) {
                        System.err.println("[Streamer] Error en listener: " + e.getMessage());
                    }
                });

                //IMPORTANTE:
                /*el listener es un hilo "daemon", es decir, es de soporte. No debe impedir que el
                sistema de stream termine, pero si este termina, debe morir con el y no seguir
                esperando mas actualizaciones.

                */
               //el setter
                listener.setDaemon(true);
                listener.start();

                // Chat: conexión al ChatServer para leer y escribir mensajes del canal
                ObjectOutputStream chatOut = null;
                try {
                    Socket chatSocket = new Socket(HOST, 6000);
                    chatOut = new ObjectOutputStream(chatSocket.getOutputStream());
                    chatOut.flush();
                    ObjectInputStream chatIn = new SafeObjectInputStream(chatSocket.getInputStream());
                    chatOut.writeObject(new ChatMessage(streamerId, channelName, "entró al chat."));
                    chatOut.flush();
                    Thread chatListener = new Thread(() -> {
                        try {
                            while (true) {
                                Object obj = chatIn.readObject();
                                if (obj instanceof ChatMessage msg) System.out.println(msg);
                            }
                        } catch (SocketException | EOFException ignored) {
                        } catch (IOException | ClassNotFoundException e) {
                            System.err.println("[Chat] Error: " + e.getMessage());
                        }
                    });
                    chatListener.setDaemon(true);
                    chatListener.start();
                } catch (IOException e) {
                    System.err.println("[Chat] No se pudo conectar al ChatServer.");
                }

                //menu de opciones luego de la confirmacion del servidor
                boolean running = true;
                while (running) {
                    System.out.println("\n--- Opciones ---");
                    System.out.println("1. Pausar stream");
                    System.out.println("2. Reanudar stream");
                    System.out.println("3. Cerrar canal");
                    System.out.println("(o escribe un mensaje para el chat)");
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
                            //se muere
                            running = false;
                        }
                        default -> {
                            if (!option.isBlank() && chatOut != null) {
                                try {
                                    ChatMessage msg = new ChatMessage(streamerId, channelName, option);
                                    chatOut.writeObject(msg);
                                    chatOut.flush();
                                    chatOut.reset();
                                    System.out.println(msg.toString());
                                } catch (IOException e) {
                                    System.err.println("[Chat] Error al enviar.");
                                }
                            }
                        }
                    }
                }

            } catch (IOException | ClassNotFoundException e) {
                System.err.println("[Streamer] Error de conexión: " + e.getMessage());
            }
        }
    }
}
