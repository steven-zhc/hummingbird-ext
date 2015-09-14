package com.hczhang.hummingbird.ext.eventrouter.disruptor;

import com.codahale.metrics.Timer;
import com.hczhang.hummingbird.event.Event;
import com.hczhang.hummingbird.event.Handler;
import com.hczhang.hummingbird.eventbus.EventBus;
import com.hczhang.hummingbird.metrics.MetricsManager;
import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by steven on 1/30/15.
 */
public class EventPayloadHandler implements EventHandler<EventPayload> {

    private static Logger logger = LoggerFactory.getLogger(EventPayloadHandler.class);

    private Timer timer;

    private Handler handler;

    public EventPayloadHandler(Handler handler) {
        this.handler = handler;

        timer = MetricsManager.metrics.timer(name(EventBus.class, handler.getClass().getSimpleName(), "timer"));
    }

    @Override
    public void onEvent(EventPayload eventPayload, long sequence, boolean endOfBatch) throws Exception {

        Timer.Context context = MetricsManager.getTimerContext(timer);

        Event e = eventPayload.getEvent();
        try {
            logger.info("Event handler [{}] is working on event [{}]", handler.getClass().getSimpleName(), e.getClass().getSimpleName());
            handler.handle(e);

        } catch (Exception ex) {
            logger.error("Got a exception when EventHandler[{}] was working on Event [{}].",
                    handler.getClass().getSimpleName(), e.getClass().getSimpleName(), ex);
        } finally {
            MetricsManager.stopTimerContext(context);
        }
    }
}
