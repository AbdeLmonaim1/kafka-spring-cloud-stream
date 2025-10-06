package ma.sdia.kafkaspringcloudstream.events;

import java.util.Date;

public record PageEvent(String name, String user, Date date, Long duration) {
}
