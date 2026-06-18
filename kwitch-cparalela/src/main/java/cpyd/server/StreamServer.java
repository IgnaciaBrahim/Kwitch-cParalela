package cpyd.server;

import cpyd.handler.ClientHandler;
import cpyd.model.StreamSession;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

//objetivo

/*es el punto de entrada del servidor. Su trabajo es estar permanentemente escuchando conexiones 
que quieren entrar al puerto 5000. 

Si llega un cliente nuevo, se crea un ClientHandler y lo lanza en un hilo separado y queda esperando 
al siguiente cliente. 

Aquí se encuentran las estructuras de datos compartidas entre server y clientes (activeSessions y 
subscribers) porque todos los hilos necesitan acceder a ellas, y están aquí por la concurrencia. 

*/

public class StreamServer {

    //puerto de conexion 
    public static final int PORT = 5000;

    //mapa sesiones de canal activo
    public static final ConcurrentHashMap<String, StreamSession> activeSessions = 
                                                    new ConcurrentHashMap<>();

    //lista de gestores de espectadores suscritos (conectados) para cada canal activo
    public static final ConcurrentHashMap<String, CopyOnWriteArrayList<ClientHandler>> subscribers = 
                                                                        new ConcurrentHashMap<>();

    public static void main(String[] args) {
        System.out.println("[StreamServer] Se esta iniciando en el puerto " + PORT);

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {

            while (true) {

                Socket clientSocket = serverSocket.accept();
                System.out.println("[StreamServer] Cliente conectado: " + clientSocket.getInetAddress());

                //el clienthandler ahora se va a encargar del cliente conectado
                ClientHandler handler = new ClientHandler(clientSocket);
                Thread thread = new Thread(handler); //hilo
                thread.start();
            }

        } catch (IOException e) {
            System.err.println("[StreamServer] Error fatal del servidor : " + e.getMessage());
        }
    }
}
