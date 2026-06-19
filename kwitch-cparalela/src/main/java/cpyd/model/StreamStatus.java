package cpyd.model;

//objetivo:

/*una lista de valores posibles para el estado del stream. 

En vez de usar strings que son propensas a errores de tipeo, definimos los estados válidos una vez 
y el compilador va a avisar si se usa un estado inexistente para el sistema.

*/

public enum StreamStatus {LIVE, PAUSED, ENDED}

