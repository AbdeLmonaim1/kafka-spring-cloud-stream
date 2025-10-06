package ma.sdia.kafkaspringcloudstream.controller;

import ma.sdia.kafkaspringcloudstream.events.PageEvent;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.Random;

@RestController
public class PageEventController {
    //Allows you to publish a message in any Broker, Kafka, or other topic.
    private StreamBridge streamBridge;
    public PageEventController(StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
    }

    @GetMapping("/publish")
    public PageEvent publish(String name, String topic){
        PageEvent pageEvent = new PageEvent(name,
                Math.random()>0.5?"Monaim":"Amine",
                new Date(),
                (long) (10+new Random().nextInt(10000)));
        streamBridge.send(topic, pageEvent);
        return pageEvent;
    }
}
