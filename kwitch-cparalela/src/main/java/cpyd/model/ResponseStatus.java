package cpyd.model;

/*una lista de valores posibles para las respuestas del servidor (como en StreamStatus). 

Se va a utilizar para definir si una operación resultó bien (OK), mal (ERROR), o si el canal fue 
cerrado (CHANNEL_CLOSED). Evita que el servidor responda strings a los que el cliente tenga que 
comparar con equals() (errores)

*/

public enum ResponseStatus {OK, ERROR, CHANNEL_CLOSED}