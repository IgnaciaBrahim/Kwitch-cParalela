package cpyd.distributed;

import java.io.Serializable;

//objetivo:

/*es un wrapper que envuelve cualquier mensaje que viaje entre nodos,
pero con el timestamp de Lamport incluido.

Asi el que recibe sabe en que momento logico se envio y puede actualizar
su propio reloj con update(). 
*/

public class MessageWithClock implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int lamportTime;
    private final String senderId;
    private final String type;
    private final Object payload;

    public MessageWithClock(int lamportTime, String senderId, String type, Object payload) {
        this.lamportTime = lamportTime;
        this.senderId = senderId;
        this.type = type;
        this.payload = payload;
    }

    public int getLamportTime()      { return lamportTime; }
    public String getSenderId()      { return senderId; }
    public String getType()          { return type; }
    public Object getPayload()       { return payload; }
}
