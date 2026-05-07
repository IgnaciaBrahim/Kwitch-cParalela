package cpyd;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;

public class ViewerClient {
    private static final String STREAM_SERVER_HOST = "127.0.0.1";
    private static final int STREAM_SERVER_PORT = 5000;
    private static final int RECONNECT_DELAY_MS = 3000;
    
    private final String channelToJoin;

    public ViewerClient(String channelToJoin) {
        this.channelToJoin = channelToJoin;
    }

    public void startListening() {
        boolean connected = false;
        
        // Reconexión del cliente ante caída del servidor (retry logic básico)
        while (!connected) {
            try (Socket socket = new Socket(STREAM_SERVER_HOST, STREAM_SERVER_PORT)) {
                System.out.println("Conectado al Stream Server. Suscribiéndose al canal: " + channelToJoin);
                
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                // Suscripción al canal enviando un ServerResponse a modo de comando
                ServerResponse subscribeRequest = new ServerResponse("SUBSCRIBE", channelToJoin, null);
                out.writeObject(subscribeRequest);
                out.flush();

                //  Recepción de notificaciones de estado
                while (true) {
                    Object obj = in.readObject();
                    if (obj instanceof StreamSession) {
                        StreamSession session = (StreamSession) obj;
                        System.out.println("[STREAM UPDATE] Canal: " + session.getChannelName() + " | Estado: " + session.getStatus());
                    } else if (obj instanceof ServerResponse) {
                        ServerResponse response = (ServerResponse) obj;
                        System.out.println("[SERVER] " + response.getMessage());
                        if ("CHANNEL_CLOSED".equals(response.getMessage())) {
                            System.out.println("Transmisión finalizada por el streamer.");
                            return; // Fin de la ejecución si el canal se cierra de forma natural
                        }
                    }
                }

            } catch (SocketException | EOFException e) {
                // Manejo de excepciones de red ante caída repentina del Stream Server
                System.err.println("Conexión perdida (Crash del servidor): " + e.getMessage());
                reconnectDelay();
            } catch (IOException | ClassNotFoundException e) {
                System.err.println("Error de I/O o deserialización en el cliente: " + e.getMessage());
                reconnectDelay();
            }
        }
    }

    private void reconnectDelay() {
        System.out.println("Reintentando conexión en " + (RECONNECT_DELAY_MS / 1000) + " segundos...");
        try {
            Thread.sleep(RECONNECT_DELAY_MS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) {
        new ViewerClient("KwitchChannel1").startListening();
    }
}