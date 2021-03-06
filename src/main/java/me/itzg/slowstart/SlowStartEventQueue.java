package me.itzg.slowstart;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.NameBasedGenerator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.Adler32;

/**
 * This is a specialized queuing construct that is intended for a two-phase
 * scenario where in the first-phase events need to be streamed to disk until the
 * consumer is ready for those events. After readiness is reached, the events are
 * delivered to the consumer in the original order by draining the slow-start
 * buffer and then seamless switching to pass-through of ongoing produced events.
 *
 * @author Geoff Bourne
 */
public class SlowStartEventQueue {
    private volatile IOException lastException;

    public static class Stats {
        long timeToDrainNS;
        LongAdder preReady = new LongAdder();
        LongAdder preDrained = new LongAdder();
        LongAdder total = new LongAdder();
        LongAdder drained = new LongAdder();

        public long getPreReady() {
            return preReady.sum();
        }

        public long getPreDrained() {
            return preDrained.sum();
        }

        public long getTotal() {
            return total.sum();
        }

        public long getDrained() {
            return drained.sum();
        }

        public long getTimeToDrainNS() {
            return timeToDrainNS;
        }
    }

    private static final NameBasedGenerator keyUuidGen = Generators.nameBasedGenerator();

    private static final Logger log = Logger.getLogger(SlowStartEventQueue.class.getName());

    private final Stats stats = new Stats();
    private final Path storePath;
    private final String key;
    private final Executor executor;
    private final EventConsumer consumer;

    private static final int STATE_INITIAL = 0;
    private static final int STATE_PENDING_SLOW_START = 1;
    private static final int STATE_SLOW_START = 2;
    private static final int STATE_DRAINING = 3;
    private static final int STATE_DRAIN_DONE = 4;
    private static final int STATE_STEADY = 5;
    private AtomicInteger state = new AtomicInteger();

    private volatile FileChannel slowStoreIn;
    private FileChannel slowStoreOut;

    /**
     * Creates a queue that can immediately accept calls to {@link #publish(ByteBuffer)}; however, it starts
     * initially in a "slow-start" phase.
     *
     * @param key         this is the key used to derive a segregated slow-start buffer area under the given <code>storagePath</code>
     * @param consumer    a consumer that will not be given events until {@link #ready()} is indicated. The consumer
     *                    accepts both the <code>key</code> provided and the content of the event. That allows for one
     *                    consumer instance to be shared across multiple {@link SlowStartEventQueue}.
     *                    This consumer may either
     *                    be invoked within a thread from <code>executor</code> or within the calling thread depending on
     *                    the phase of the queue.
     * @param storagePath the path under which key-specific start-start buffer directories are created. This directory
     *                    and its parents will be created, if absent
     * @param executor    used for executing the slow-start draining thread
     * @throws IOException when the key-specific slow-start buffer directory cannot be created
     */
    public SlowStartEventQueue(String key, EventConsumer consumer, Path storagePath, Executor executor) throws IOException {
        this.key = key;
        this.executor = executor;
        this.consumer = consumer;
        final Path keyStoragePath = storagePath.resolve(keyUuidGen.generate(key).toString());
        Files.createDirectories(keyStoragePath);

        storePath = keyStoragePath.resolve("store.dat");
    }

    /**
     * Used only to create a purposely non-functional queue to propagate errors during creation.
     * @param e
     */
    SlowStartEventQueue(IOException e) {
        key = null;
        executor = null;
        consumer = null;
        storePath = null;
        this.lastException = e;
    }

    /**
     * Used for the router to recall the reason for invalid queue creation.
     * @return the exception passed at creation or null if this queue is valid
     */
    IOException getLastException() {
        return lastException;
    }

    /**
     * Used by the router to see if a non-functional queue was created.
     * @return true if valid for use
     */
    boolean isValid() {
        return key != null;
    }

    /**
     * Events are published via this method. Depending on the readiness of the queue, events will either be streamed
     * to the slow-start buffer and/or passed directly to the <code>consumer</code>. This method is thread-safe.
     *
     * @param payload the opaque content of the event that needs to be rewound prior to this call
     * @throws IOException if the slow-start buffer is currently in use and an I/O operation fails
     */
    public void publish(ByteBuffer payload) throws IOException {
        if (payload == null || payload.remaining() == 0) {
            throw new IllegalArgumentException("payload is absent or empty");
        }

        stats.total.add(1);

        if (state.get() < STATE_DRAIN_DONE) {
            stats.preDrained.add(1);

            if (state.get() == STATE_INITIAL) {
                if (state.compareAndSet(STATE_INITIAL, STATE_SLOW_START)) {
                    openSlowStoreIn();
                }
            }

            final ByteBuffer header = ByteBuffer.allocate(12);
            final Adler32 adler32 = new Adler32();
            adler32.update(payload);
            payload.rewind();

            header.putInt(payload.remaining());
            header.putLong(adler32.getValue());
            header.rewind();

            slowStoreIn.write(new ByteBuffer[]{header, payload});
            return;
        }
        // this is the ack to the reader seeing that the draining of the slow start caught up
        else if (state.compareAndSet(STATE_DRAIN_DONE, STATE_STEADY)) {
            slowStoreIn.close();
        }

        consumer.consume(key, payload);
    }

    private void openSlowStoreIn() throws IOException {
        slowStoreIn = FileChannel.open(storePath,
                                       StandardOpenOption.CREATE,
                                       // append to allow for rudimentary resuming of the slow-store
                                       StandardOpenOption.APPEND,
                                       StandardOpenOption.WRITE);
    }

    /**
     * An appropriate external user of this queue calls this method to indicate that events can now be delivered
     * to the <code>consumer</code>. This method is thread-safe especially with regard to conccurent invocation of
     * {@link #publish(ByteBuffer)}.
     */
    public void ready() {
        if (state.compareAndSet(STATE_SLOW_START, STATE_DRAINING)) {
            // snap the stats here
            stats.preReady.add(stats.preDrained.sum());
            executor.execute(this::drainSlowStore);
        } else {
            state.compareAndSet(STATE_INITIAL, STATE_STEADY);
        }
    }

    private void drainSlowStore() {
        final ByteBuffer header = ByteBuffer.allocate(12);

        try {
            slowStoreOut = FileChannel.open(storePath,
                                            StandardOpenOption.READ, StandardOpenOption.DELETE_ON_CLOSE);

            final long startTS = System.nanoTime();
            while (true) {
                if (slowStoreOut.read(header) < 12) {
                    if (state.compareAndSet(STATE_DRAINING, STATE_DRAIN_DONE)) {
                        stats.timeToDrainNS = System.nanoTime() - startTS;
                        slowStoreOut.close();
                    }
                    return;
                }
                header.rewind();

                final int len = header.getInt();
                if (len == 0) {
                    throw new IllegalStateException("Read a zero length");
                }
                final long checksum = header.getLong();
                header.rewind();

                final ByteBuffer buf = ByteBuffer.allocate(len);
                while (buf.remaining() > 0) {
                    slowStoreOut.read(buf);
                }
                buf.rewind();

                final Adler32 adler32 = new Adler32();
                adler32.update(buf);
                buf.rewind();

                stats.drained.add(1);
                if (checksum == adler32.getValue()) {
                    consumer.consume(key, buf);
                } else {
                    log.log(Level.SEVERE, String.format("Block with length=%d failed checksum", len));
                }
            }
        } catch (IOException e) {
            log.log(Level.SEVERE, "Unable to read file store", e);
        }
    }

    /**
     * Provides some running operational stats about the queue.
     * @return the queue's stats
     */
    public Stats getStats() {
        return stats;
    }
}
