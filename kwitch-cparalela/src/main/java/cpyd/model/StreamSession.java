package cpyd.model;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;


/*es el objeto principal que viaja por el socket desde el Streamer hacia el servidor. Encapsula toda
la información de un canal activo: quién lo abre, cómo se llama, qué está mostrando, y en qué 
estado está. 

Es lo que el servidor registra cuando un streamer inicia un canal, y lo que los 
espectadores reciben cuando se suscriben.

*/

public class StreamSession implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String streamerId;
    private final String channelName;
    private final String description;
    private String currentApp;
    private final List<String> tags;
    private StreamStatus status;
    private final LocalDateTime startTime;
    private int viewerCount;

    public StreamSession(String streamerId, String channelName,
                         String description, String currentApp, List<String> tags) {
        this.streamerId  = streamerId;
        this.channelName = channelName;
        this.description = description;
        this.currentApp  = currentApp;
        this.tags        = tags;
        this.status      = StreamStatus.LIVE;
        this.startTime   = LocalDateTime.now();
        this.viewerCount = 0;
    }

    // Getters y setters
    public String getStreamerId()          { return streamerId; }
    public String getChannelName()         { return channelName; }
    public String getDescription()         { return description; }
    public String getCurrentApp()          { return currentApp; }
    public List<String> getTags()          { return tags; }
    public StreamStatus getStatus()        { return status; }
    public LocalDateTime getStartTime()    { return startTime; }
    public int getViewerCount()            { return viewerCount; }

    public void setStatus(StreamStatus status)       { this.status = status; }
    public void setViewerCount(int viewerCount)      { this.viewerCount = viewerCount; }
    public void setCurrentApp(String currentApp)     { this.currentApp = currentApp; }
}
