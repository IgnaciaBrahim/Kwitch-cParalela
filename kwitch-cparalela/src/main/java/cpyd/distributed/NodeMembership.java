package cpyd.distributed;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


/*lleva la lista de nodos que estan vivos en el sistema.

Cada nodo (StreamServer, ChatServer, CoordinatorNode) se registra aca
con su id, ip y puerto. Los heartbeats mantienen actualizado el estado
entre ALIVE y FAILED.

Cuando un nodo no responde por un tiempo, se marca FAILED y los demas
saben que no pueden contar con el para coordinarse (Ricart-Agrawala).

*/

public class NodeMembership {

    //un nodo se representa con su info basica
    public static class NodeInfo {
        private final String id;
        private final String host;
        private final int port;
        private volatile String status; // "ALIVE" o "FAILED"

        public NodeInfo(String id, String host, int port) {
            this.id = id;
            this.host = host;
            this.port = port;
            this.status = "ALIVE";
        }

        public String getId()       { return id; }
        public String getHost()     { return host; }
        public int getPort()        { return port; }
        public String getStatus()   { return status; }

        public void setStatus(String status) { this.status = status; }
    }

    //mapa: id del nodo -> su info
    private final ConcurrentHashMap<String, NodeInfo> nodes = new ConcurrentHashMap<>();

    //agrega un nodo a la membresia
    public void registerNode(String id, String host, int port) {
        nodes.put(id, new NodeInfo(id, host, port));
    }

    //marca un nodo como caido
    public void markFailed(String id) {
        NodeInfo node = nodes.get(id);
        if (node != null) {
            node.setStatus("FAILED");
        }
    }

    //marca un nodo como vivo otra vez (cuando se reconecta)
    public void markAlive(String id) {
        NodeInfo node = nodes.get(id);
        if (node != null) {
            node.setStatus("ALIVE");
        }
    }

    //saca un nodo de la membresia
    public void removeNode(String id) {
        nodes.remove(id);
    }

    //obtiene la info de un nodo
    public NodeInfo getNode(String id) {
        return nodes.get(id);
    }

    //devuelve los ids de los nodos que estan vivos
    public List<String> getAliveNodes() {
        List<String> alive = new ArrayList<>();
        for (NodeInfo node : nodes.values()) {
            if ("ALIVE".equals(node.getStatus())) {
                alive.add(node.getId());
            }
        }
        return alive;
    }

    //devuelve todos los ids (vivos y caidos)
    public List<String> getAllNodes() {
        return new ArrayList<>(nodes.keySet());
    }

    //cantidad total de nodos registrados
    public int size() {
        return nodes.size();
    }
}
