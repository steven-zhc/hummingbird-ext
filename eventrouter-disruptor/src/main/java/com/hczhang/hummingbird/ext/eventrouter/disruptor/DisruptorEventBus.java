package com.hczhang.hummingbird.ext.eventrouter.disruptor;

import com.codahale.metrics.Histogram;
import com.hczhang.hummingbird.event.Event;
import com.hczhang.hummingbird.event.Handler;
import com.hczhang.hummingbird.eventbus.EventBus;
import com.hczhang.hummingbird.metrics.MetricsManager;
import com.hczhang.hummingbird.util.DDDException;
import com.lmax.disruptor.dsl.Disruptor;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by steven on 1/30/15.
 */
public class DisruptorEventBus implements EventBus {

    private static Logger logger = LoggerFactory.getLogger(DisruptorEventBus.class);

    private Class<? extends Event> type;

    private Disruptor<EventPayload> disruptor;

    private int bufferSize;

    private Histogram histogram;

    public DisruptorEventBus(Class<? extends Event> eventType, int bufferSize) {

        this.type = eventType;
        this.bufferSize = bufferSize;

        histogram = MetricsManager.getHistogram(name(EventBus.class, "queue", type.getSimpleName(), "size"));

    }

    @Override
    public void init() {
        disruptor = new Disruptor<EventPayload>(
                new EventPayloadFactory(),
                bufferSize,
                Executors.newCachedThreadPool()
        );
    }

    @Override
    public void subscribe(Handler<? extends Event> handler) {
        Validate.notNull(handler, "Handler is null");

        logger.debug("Subscribe handler : [{}]", handler.getClass().getSimpleName());
        disruptor.handleEventsWith(new EventPayloadHandler(handler));
    }

    @Override
    public void unsubscribe(Handler<? extends Event> handler) {
        Validate.notNull(handler, "Handler is null");

        throw new DDDException("DisruptorEventBus doesn't support unsubscribe handler function");
    }

    @Override
    public void publish(Event event) {
        disruptor.publishEvent(new EventPlaylodTranslatorOneArg(), event);
        MetricsManager.updateHistogram(histogram, getBusSize());
    }


    @Override
    public boolean startup() {
        this.disruptor.start();

        logger.info("Start EventBus<{}>[{}] ...", type.getSimpleName(), bufferSize);

        return true;
    }

    @Override
    public boolean shutdown() {
        disruptor.shutdown();
        return true;
    }

    @Override
    public long getBusSize() {
        return bufferSize - disruptor.getRingBuffer().remainingCapacity();
    }

    @Override
    public Class getEventType() {
        return type;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }
}
