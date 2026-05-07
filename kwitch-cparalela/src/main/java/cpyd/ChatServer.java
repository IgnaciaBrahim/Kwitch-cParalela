package cpyd;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ChatServer {
    private static final int PORT = 6000;

    // Sincronización sobre mapa de usuarios por canal.
    // Se recomienda usar ConcurrentHashMap + CopyOnWriteArrayList en lugar de bloques synchronized 
    // para evitar cuellos de botella en operaciones de lectura masiva (broadcast).
    private final Map<String, List<ChatClientHandler>> channels = new ConcurrentHashMap<>();
    
    // Evaluación e implementación de ExecutorService.
    // Se recomienda newCachedThreadPool() para cargas I/O porque reutiliza hilos inactivos y crea nuevos solo si es necesario.
    private final ExecutorService threadPool = Executors.newCachedThreadPool();

    public static void main(String[] args) {
        new ChatServer().startServer();
    }

    public void startServer() {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("ChatServer escuchando en el puerto " + PORT + "...");

            while (true) {
                Socket clientSocket = serverSocket.accept();
                ChatClientHandler handler = new ChatClientHandler(clientSocket, this);
                
                // Delegamos el manejador al pool en lugar de instanciar hilos manualmente
                threadPool.execute(handler);
            }
        } catch (IOException e) {
            System.err.println("Fallo crítico en el puerto del ChatServer: " + e.getMessage());
        } finally {
            threadPool.shutdown();
        }
    }

    public void addClientToChannel(String channelId, ChatClientHandler client) {
        channels.computeIfAbsent(channelId, k -> new CopyOnWriteArrayList<>()).add(client);
    }

    public void removeClientFromChannel(String channelId, ChatClientHandler client) {
        List<ChatClientHandler> channelClients = channels.get(channelId);
        if (channelClients != null) {
            channelClients.remove(client);
            // Liberar memoria si el canal queda vacío
            if (channelClients.isEmpty()) {
                channels.remove(channelId);
            }
        }
    }

    public List<ChatClientHandler> getChannelClients(String channelId) {
        return channels.get(channelId);
    }
}