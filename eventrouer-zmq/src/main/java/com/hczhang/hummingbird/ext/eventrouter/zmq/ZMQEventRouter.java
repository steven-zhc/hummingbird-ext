package com.hczhang.hummingbird.ext.eventrouter.zmq;

import com.hczhang.hummingbird.event.Event;
import com.hczhang.hummingbird.eventbus.AbstractEventRouter;
import com.hczhang.hummingbird.eventbus.EventBus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;

/**
 * Created by steven on 9/5/14.
 */
public class ZMQEventRouter extends AbstractEventRouter {

    @Autowired
    private AutowireCapableBeanFactory factory;

    @Override
    protected EventBus createEventBus(Class<? extends Event> eventType) {
        EventBus bus = new ZMQEventBus(eventType.getName(), eventType);
        factory.autowireBean(bus);
        return bus;
    }

}
