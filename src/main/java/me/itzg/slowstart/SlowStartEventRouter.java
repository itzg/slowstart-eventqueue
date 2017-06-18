package me.itzg.slowstart;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is a construct that automatically manages {@link SlowStartEventQueue} instances by creating them upon
 * observation of a new key and likewise notifying an observer.
 *
 * @author Geoff Bourne
 */
public class SlowStartEventRouter {
    private static final Logger log = Logger.getLogger(SlowStartEventRouter.class.getName());

    private final NewKeyObserver newKeyObserver;
    private final EventConsumer consumer;
    private final Path storagePath;
    private final Executor executor;
    private ConcurrentHashMap<String, SlowStartEventQueue> routes = new ConcurrentHashMap<>();

    public SlowStartEventRouter(NewKeyObserver newKeyObserver, EventConsumer consumer, Path storagePath, Executor executor) {
        this.newKeyObserver = newKeyObserver;
        this.consumer = consumer;
        this.storagePath = storagePath;
        this.executor = executor;
    }

    /**
     * This will route the event's payload to an existing route or create a new route, as needed.
     * This method is thread-safe.
     *
     * @param key the event's key
     * @param payload the opaque content of the event
     * @throws IOException
     */
    public void route(String key, ByteBuffer payload) throws IOException {
        final SlowStartEventQueue queue = routes.computeIfAbsent(key, this::createNewRoute);

        if (queue.isValid()) {
            queue.publish(payload);
        }
        else {
            // this will get noisy since re-thrown on every routing of the given key, but being noisy is probably good
            throw queue.getLastException();
        }

    }

    private SlowStartEventQueue createNewRoute(String key) {
        final SlowStartEventQueue queue;
        try {
            queue = new SlowStartEventQueue(key, consumer, storagePath, executor);
        } catch (IOException e) {
            log.log(Level.SEVERE, "Unable to create new route", e);
            return new SlowStartEventQueue(e);
        }

        CompletableFuture<Void> future = newKeyObserver.observeNewKey(key);
        future.thenAccept((v) -> queue.ready());

        return queue;
    }
}
