package cpyd.distributed;

import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/*
Lector de objetos seguro para las conexiones de red.

La serializacion nativa de Java permite que quien manda los bytes haga que la
JVM cargue clases arbitrarias del classpath, lo que abre la puerta a ejecucion
remota de codigo. Para evitarlo, solo dejamos deserializar clases de los
paquetes que realmente usamos:

  - cpyd.*      objetos propios del sistema (StreamSession, ChatMessage, etc.)
  - java.lang.* String, Integer, Long, enums...
  - java.util.* List/ArrayList y la lista de tags
  - java.time.* LocalDateTime

Si llega cualquier otra clase, o un proxy dinamico, se rechaza con
InvalidClassException antes de instanciar nada.
*/
public class SafeObjectInputStream extends ObjectInputStream {

    private static final String[] PREFIJOS_PERMITIDOS = {
        "cpyd.",
        "java.lang.",
        "java.util.",
        "java.time."
    };

    public SafeObjectInputStream(InputStream in) throws IOException {
        super(in);
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc)
            throws IOException, ClassNotFoundException {

        String nombre = desc.getName();

        //un array se describe como "[Lcpyd.Foo;" o "[I" (primitivos);
        //quitamos los corchetes y la envoltura L...; para mirar el tipo base
        String base = nombre;
        while (base.startsWith("[")) {
            base = base.substring(1);
        }
        if (base.startsWith("L") && base.endsWith(";")) {
            base = base.substring(1, base.length() - 1);
        }

        //tipos primitivos (int, double, boolean...) no llevan paquete: se permiten
        if (!base.contains(".")) {
            return super.resolveClass(desc);
        }

        for (String prefijo : PREFIJOS_PERMITIDOS) {
            if (base.startsWith(prefijo)) {
                return super.resolveClass(desc);
            }
        }

        throw new InvalidClassException(
            "Deserializacion bloqueada: clase no permitida -> " + nombre);
    }

    @Override
    protected Class<?> resolveProxyClass(String[] interfaces)
            throws IOException, ClassNotFoundException {
        throw new InvalidClassException(
            "Deserializacion bloqueada: proxies dinamicos no permitidos");
    }
}
