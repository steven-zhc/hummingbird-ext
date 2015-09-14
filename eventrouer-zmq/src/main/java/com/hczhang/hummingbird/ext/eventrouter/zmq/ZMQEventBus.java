package com.hczhang.hummingbird.ext.eventrouter.zmq;

import com.hczhang.hummingbird.event.Event;
import com.hczhang.hummingbird.eventbus.AbstractEventBus;
import com.hczhang.hummingbird.serializer.JsonSerializer;
import com.hczhang.hummingbird.serializer.Serializer;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Created by steven on 9/5/14.
 */
public class ZMQEventBus extends AbstractEventBus {

    private static Logger logger = LoggerFactory.getLogger(ZMQEventBus.class);

    private ExecutorService pool;

    private ZMQ.Context context;
    private ZMQ.Socket publisher;
    private ZMQ.Socket console;

    private Serializer serializer = new JsonSerializer();
    private Class<? extends Event> type;

    public ZMQEventBus(final String busName, Class<? extends Event> eventType) {
        type = eventType;

        pool = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                return new Thread(runnable, busName + "-Thread");
            }
        });

        context = ZMQ.context(0);
        publisher = context.socket(ZMQ.PUB);
        publisher.bind("inproc://" + busName);

        console = context.socket(ZMQ.REP);
        console.bind("inproc://" + busName + "/sync");

        pool.submit(new QueueClient(context, busName));

        int clientNum = 0;
        while (clientNum < 1) {
            console.recv(0);
            console.send("START");
            clientNum++;
        }
        logger.debug("Waiting for messages on inproc://{}", busName);
    }

    @Override
    public Class getEventType() {
        return this.type;
    }

    @Override
    public void publish(Event event) {
        if (getListeners().size() == 0) {
            return;
        }
        byte[] msg = serializer.serialize(event);
        logger.debug("Sending message: [{}] ", new String(msg));
        publisher.send(msg);
    }

    @Override
    public boolean startup() {
        return true;
    }

    @Override
    public boolean shutdown() {
        pool.shutdown();
        context.close();
        return true;
    }

    @Override
    public long getBusSize() {
        return console.getReceiveBufferSize();
    }

    class QueueClient implements Runnable {

        private ZMQ.Context context;
        private String name;

        public QueueClient(ZMQ.Context cxt, String n) {
            Validate.notNull(cxt, "Context must not be null.");
            Validate.notNull(n, "Queue name must not be null.");

            context = cxt;
            name = n;
        }

        @Override
        public void run() {

            ZMQ.Socket socket = context.socket(ZMQ.SUB);
            socket.connect("inproc://" + name);
            socket.subscribe("".getBytes());

            ZMQ.Socket syncClient = context.socket(ZMQ.REQ);
            syncClient.connect("inproc://" + name + "/sync");

            syncClient.send("READY".getBytes(), 0);
            syncClient.recv(0);

            ZMQ.Poller poller = new ZMQ.Poller(1);
            poller.register(socket, ZMQ.Poller.POLLIN);

            logger.debug("Event Bus<{}> [The current bus is {}] is running ...", type.getSimpleName(), ZMQEventBus.class.getSimpleName());

            while (!pool.isShutdown()) {

                if (poller.poll(1 * 1000l) > 0 && poller.pollin(0)) {

                    // Wait for next request from the client
                    byte[] request = socket.recv(0);

                    Event event = serializer.deserialize(request, type);

                    logger.debug("Retrieve an event from bus. And retrieved handler [{}] " + event.getClass().getName(), getListeners().size());

                    handle(event);
                }
            }
            socket.close();

            logger.debug("Event queue is shutting down ...");
        }
    }
}
