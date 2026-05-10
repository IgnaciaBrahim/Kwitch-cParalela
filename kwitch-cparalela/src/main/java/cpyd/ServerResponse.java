package cpyd;

//imports
import java.io.Serializable;

//objetivo:

/*es el la respuesta (un objeto) que el servidor le devuelve al cliente después de 
cualquier operación. 

Tiene tres campos: el status (si salió bien o mal), un mensaje para mostrar en 
consola, y un payload que es la StreamSession actualizada cuando el servidor tiene algo 
que devolver:

por ejemplo: cuando un espectador se suscribe y el servidor le envía el estado actual 
del canal. 

El payload puede ser null, como cuando el servidor simplemente confirma que recibió 
algo. 

*/

public class ServerResponse implements Serializable {

    /*el UID es un identificador que Java usa para verificar que el objeto serializado y la 
    clase que lo deserializa son compatibles, si no existe puede haber una excepción
    */
    private static final long serialVersionUID = 1L; 

    private final ResponseStatus status;  //estado actual 
    private final String message; //mensajes
    private final StreamSession payload;  //puede ser null: cuando el servidor responde un error no tiene 
                                    // sesión que devolver. 

    //constructor
    public ServerResponse(ResponseStatus status, String message, StreamSession payload) {
        this.status  = status;
        this.message = message;
        this.payload = payload;
    }

    //getters 
    public ResponseStatus getStatus()    { return status; }
    public String getMessage()           { return message; }
    public StreamSession getPayload()    { return payload; }
}
