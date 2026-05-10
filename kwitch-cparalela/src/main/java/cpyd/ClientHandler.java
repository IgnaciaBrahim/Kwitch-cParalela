package cpyd;

//imports
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

//objetivo
/*este es un hilo que atiende al cliente conectado a traves de la sesion. 
Primero identifica que tipo de cliente es según el primer mensaje que recibe

ejemplo: si llega una StreamSession es un streamer, si llega un String es un espectador.

Luego ejecuta la lógica correspondiente al rol. Si es streamer, registra el canal y escucha 
actualizaciones de estado. Si es espectador, lo suscribe al canal y queda esperando notificaciones.

Finalmente limpia cuando el cliente se desconecta para que no queden guardados clientes que ya no 
estan conectados, sin importar cómo ocurrió esa desconexión.
*/

//no va a extender thread ya que es más util que extienda otros objetos si es necesario.
public class ClientHandler implements Runnable {

    private final Socket socket;    //su propio socket, lo recibe cuando el server lo acepta
                                    //en StreamServer

    private ObjectInputStream in;   //lector
    private ObjectOutputStream out; //escritor

    private String role;            //hay 2 roles de clientes, el streamer y el viewer

    private String channelName;     //canal al que pertenece este cliente. En este caso va a funcionar
                                    //solo con 1, aunque técnicamente se puede conectar a varios en 
                                    //pestañas diferentes

    public ClientHandler(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
            //el out se crea antes que el in para evitar fallos por deadlock
            //si no el cliente espera para siempre un mensaje que no va a llegar.
            socket.setSoTimeout(180000); //tiene timeout para echarle si no escucha nada del cliente
            out = new ObjectOutputStream(socket.getOutputStream());
            //no deadlock por header!
            out.flush();
            in  = new ObjectInputStream(socket.getInputStream());

            //El primer mensaje determina si es streamer o viewer o si solicita lista de canales
            Object first = in.readObject();

            if (first instanceof StreamSession session) {
                //los handler son diferentes porque a pesar de que son clientes, la vista
                //del streamer es totalmente distinta a la de viewer, y hacen cosas distintas.
                role = "STREAMER";
                handleStreamer(session);

            } else if (first instanceof String request) {
                if ("LIST_CHANNELS".equals(request)) {
                    // El cliente solo quiere ver los canales. Se envían las llaves del mapa.
                    List<String> channels = new ArrayList<>(StreamServer.activeSessions.keySet());
                    out.writeObject(channels);
                    out.flush();
                    // Termina la ejecución para que el bloque finally cierre este socket temporal
                } else {
                    // Si no es LIST_CHANNELS, asume que es el nombre del canal para suscribirse
                    role = "VIEWER";
                    handleViewer(request);
                }
            }

        } catch (SocketException | EOFException e) {
            System.out.println("[ClientHandler] El cliente se ha desconectado abruptamente.");
        } catch (SocketTimeoutException e) {
            //luego del timeout
            System.out.println("[Streamer] Conexión cerrada.");
            Thread.currentThread().interrupt();
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("[ClientHandler] Error en cliente: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("[ClientHandler] Se desconoce la excepcion.");

        } finally {
            cleanup();
        }

    }


    //la logica del steamer
    private void handleStreamer(StreamSession session) throws IOException, ClassNotFoundException {

        //obtiene el nombre del canal y se le asigna
        String channel = session.getChannelName();
        this.channelName = channel;

        //se registra la sesion como activa y se llena la lista de suscriptores
        StreamServer.activeSessions.put(channel, session);
        StreamServer.subscribers.putIfAbsent(channel, new CopyOnWriteArrayList<>());

        //msjs
        System.out.println("[StreamServer] Canal abierto: " + channel);

        //no es lo mismo que arriba; aqui al cliente se le dice que se abrio su canal.
        send(new ServerResponse(ResponseStatus.OK, "Canal abierto: " + channel, session));

        //se escuchan las actualizaciones del estado del stream
        while (true) {
            Object obj = in.readObject();
            if (!(obj instanceof StreamSession update)) break;

            //al streamer le interesa ver su nuevo conteo de vistas
            update.setViewerCount(StreamServer.subscribers.get(channel) != null
                ? StreamServer.subscribers.get(channel).size() : 0);

            //se modifican los numeros del server
            StreamServer.activeSessions.put(channel, update);

            //esto es un multicast de los cambios hacia todos los clientes suscritos (session)
            broadcast(channel, update);

            System.out.println("[StreamServer] El estado fue actualizado en '" + channel + "': " + 
                                                                                update.getStatus());
            //se ve si el estado es que terminó
            if (update.getStatus() == StreamStatus.ENDED) break;
        }
    }

    //logica del viewer
    private void handleViewer(String channelName) throws IOException, Exception {
        //nombre del canal
        this.channelName = channelName;

        //el viewer no se puede conectar a un stream de un canal que no esta en stream
        //solo hay mensaje, no hay payload
        if (!StreamServer.activeSessions.containsKey(channelName)) {
            send(new ServerResponse(ResponseStatus.ERROR, "Canal no encontrado: " + channelName, 
                                                                                                null));
            return;
        }

        //aqui si se ha encontrado, entonces se suscribe
        //como se suscribe, entonces al StreamSv se le envia en objeto cliente y nombre del canal
        StreamServer.subscribers.get(channelName).add(this);

        //se obtiene el stream y se envia el mensaje de que está ok la suscripcion
        StreamSession current = StreamServer.activeSessions.get(channelName);

        //se le notifica al viewer que se pudo suscribir
        send(new ServerResponse(ResponseStatus.OK, "Suscrito a: " + channelName, current));
        System.out.println("[StreamServer] Espectador suscrito a: " + channelName);

        //el hilo espera alguna notificacion, que puede venir como broadcast
        //solo va a terminar si detecta que el socket se cierra
        try {
            //delay
            while (!socket.isClosed()) {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break; //salir del loop si el hilo fue interrumpido
                }
            }
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            throw new Exception("[ClientHandler] Exception en el ClientHandler, thread interrumpido");
        }
    }

    //intercambio y comunicacion
    public void send(Object obj) {

        try {
            //se va a usar object porque no sabemos si es string o ServerResponse... etc
            out.writeObject(obj);
            out.flush();
            out.reset(); //evita que queden guardados mensajes antiguos
        } catch (IOException e) {
            System.err.println("[ClientHandler] No se pudo enviar el mensaje al cliente.");
        }
    }

    //multicast de updates
    private void broadcast(String channel, StreamSession update) {
         
        CopyOnWriteArrayList<ClientHandler> viewers = StreamServer.subscribers.get(channel);
        if (viewers == null) {
            return;
        }
        ServerResponse notification = new ServerResponse(update.getStatus() == StreamStatus.ENDED
                ? ResponseStatus.CHANNEL_CLOSED : ResponseStatus.OK, "Actualización del canal: " 
                                                                                + channel, update);

        for (ClientHandler viewer : viewers) {
            viewer.send(notification);
        }
    }

    //si el cliente (cualquiera) se desconecta, entonces el servidor tiene que "limpiar"
    //si no se referencia en el servidor a clientes fantasma
    private void cleanup() {

        if (channelName == null) {
            return;
        }

        if ("VIEWER".equals(role)) {

            CopyOnWriteArrayList<ClientHandler> viewers = StreamServer.subscribers.get(channelName);
            if (viewers != null) {
                viewers.remove(this);
            }
            System.out.println("[StreamServer] Espectador eliminado de: " + channelName);
        }

        if ("STREAMER".equals(role)) {

            StreamServer.activeSessions.remove(channelName);
            StreamServer.subscribers.remove(channelName);
            System.out.println("[StreamServer] Canal cerrado: " + channelName);
        }

        try { 
            socket.close(); 
        } catch (IOException ignored) {

        }
    }
}
