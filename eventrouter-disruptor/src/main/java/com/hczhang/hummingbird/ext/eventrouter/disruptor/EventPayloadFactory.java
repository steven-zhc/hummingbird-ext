package com.hczhang.hummingbird.ext.eventrouter.disruptor;

import com.lmax.disruptor.EventFactory;

/**
 * Created by steven on 1/30/15.
 */
public class EventPayloadFactory implements EventFactory<EventPayload> {

    @Override
    public EventPayload newInstance() {
        return new EventPayload();
    }
}
