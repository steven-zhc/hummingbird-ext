package com.hczhang.hummingbird.ext.eventrouter.disruptor;

import com.hczhang.hummingbird.event.Event;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Created by steven on 1/30/15.
 */
public class EventPayload {
    private Event event;

    public Event getEvent() {
        return event;
    }

    public void setEvent(Event event) {
        this.event = event;
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("event", event)
                .toString();
    }
}
