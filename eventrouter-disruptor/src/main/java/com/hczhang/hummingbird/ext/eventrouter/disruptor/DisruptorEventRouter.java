package com.hczhang.hummingbird.ext.eventrouter.disruptor;

import com.hczhang.hummingbird.event.Event;
import com.hczhang.hummingbird.eventbus.AbstractEventRouter;
import com.hczhang.hummingbird.eventbus.EventBus;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;

import javax.annotation.Resource;
import java.util.Properties;

/**
 * Created by steven on 2/2/15.
 */
public class DisruptorEventRouter extends AbstractEventRouter {

    @Value("${eventbus.disruptor.buffersize:1024}")
    private int buffersize;

    @Autowired
    private AutowireCapableBeanFactory factory;

    // TODO: replace this with Environment (spring 3.3+)
    @Resource(name = "routerConfiguration")
    private Properties config;

    @Override
    protected EventBus createEventBus(Class<? extends Event> eventType) {
        DisruptorEventBus bus = new DisruptorEventBus(eventType, buffersize);
        factory.autowireBean(bus);

        String value = config.getProperty("eventbus.disruptor.buffersize." + eventType.getSimpleName());
        if (StringUtils.isNotEmpty(value)) {
            bus.setBufferSize(Integer.parseInt(value));
        }

        bus.init();

        return bus;
    }
}
