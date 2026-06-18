package cpyd.distributed;


/*
Cada nodo tiene uno. Cuando envia un mensaje, hace tick() y adjunta ese numero.
Cuando recibe un mensaje, hace update(tiempoDelOtro) para quedarse con el mayor.
*/

public class LamportClock {
    
    private int time;

    public LamportClock() {
        this.time = 0;
    }

    //aumenta el reloj y devuelve el nuevo valor para mandarlo en un mensaje
    public synchronized int tick() {
        return ++time;
    }

    //cuando llega un mensaje de otro nodo, actualiza con su timestamp
    public synchronized void update(int otherTime) {
        time = Math.max(time, otherTime) + 1;
    }

    //para saber el valor actual sin modificarlo
    public synchronized int getTime() {
        return time;
    }
}
