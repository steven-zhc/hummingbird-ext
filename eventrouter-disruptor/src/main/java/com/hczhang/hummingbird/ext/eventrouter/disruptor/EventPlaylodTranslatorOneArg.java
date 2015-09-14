package com.hczhang.hummingbird.ext.eventrouter.disruptor;

import com.hczhang.hummingbird.event.Event;
import com.lmax.disruptor.EventTranslatorOneArg;

/**
 * Created by steven on 1/30/15.
 */
public class EventPlaylodTranslatorOneArg implements EventTranslatorOneArg<EventPayload, Event> {
    @Override
    public void translateTo(EventPayload eventPayload, long l, Event event) {
          eventPayload.setEvent(event);
    }
}
