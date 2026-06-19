package cpyd.distributed;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

//objetivo:

/*escribe logs a un archivo y tambien los muestra en consola.

Cada nodo del sistema (StreamServer, ChatServer, CoordinatorNode) tiene
el suyo. Los archivos quedan en la carpeta logs/ con el nombre del nodo.

Ejemplo: logs/node_StreamServer.log, logs/node_ChatServer.log

Sirve para que la rubrica pida "logs de la corrida con marcas logicas"
y se puedan adjuntar a la entrega.
*/

public class NodeLogger {

    private final PrintWriter writer;
    private final String nodeName;

    public NodeLogger(String nodeName) {
        this.nodeName = nodeName;
        PrintWriter w = null;
        try {
            new File("logs").mkdirs();
            w = new PrintWriter(new FileWriter("logs/node_" + nodeName + ".log", true));
        } catch (Exception e) {
            System.err.println("[Logger] No se pudo crear archivo de log para " + nodeName);
        }
        this.writer = w;
    }

    //escribe en el log y en consola
    public void log(String message) {
        if (writer != null) {
            writer.println(message);
            writer.flush();
        }
        System.out.println(message);
    }

    //escribe un error (System.err)
    public void error(String message) {
        String msg = "[ERROR] " + message;
        if (writer != null) {
            writer.println(msg);
            writer.flush();
        }
        System.err.println(msg);
    }

    //escribe con marca Lamport
    public void logLamport(int lamportTime, String message) {
        log("[LAMPORT=" + lamportTime + "] [" + nodeName + "] " + message);
    }

    //cierra el archivo
    public void close() {
        if (writer != null) {
            writer.close();
        }
    }
}
