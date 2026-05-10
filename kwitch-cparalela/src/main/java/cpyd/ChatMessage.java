package cpyd;

import java.io.Serializable;

public class ChatMessage implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private final String user;
    private final String channelId;
    private final String content;
    private final long timestamp;

    public ChatMessage(String user, String channelId, String content) {
        this.user = user;
        this.channelId = channelId;
        this.content = content;
        // Se asume el timestamp en el momento de creación del objeto
        this.timestamp = System.currentTimeMillis();
    }

    public String getUser() { return user; }
    public String getChannelId() { return channelId; }
    public String getContent() { return content; }
    public long getTimestamp() { return timestamp; }

    @Override
    public String toString() {
        return String.format("[%s] %s: %s", channelId, user, content);
    }
}
