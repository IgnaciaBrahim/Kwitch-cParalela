package cpyd.model;

import java.io.Serializable;

//mensaje de chat que viaja entre viewer y ChatServer

public class ChatMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String user;       //quien envia el mensaje
    private final String channelId;  //canal al que pertenece
    private final String content;    //texto del mensaje
    private final long timestamp;    //momento en que se creo

    public ChatMessage(String user, String channelId, String content) {
        this.user = user;
        this.channelId = channelId;
        this.content = content;
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
