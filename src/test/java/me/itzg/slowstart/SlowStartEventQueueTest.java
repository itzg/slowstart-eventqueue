package me.itzg.slowstart;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;

/**
 * Created by geoff on 6/17/17.
 */
public class SlowStartEventQueueTest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();
    private ExecutorService executor;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdown();
    }

    @Test(timeout = 5000)
    public void testBasic() throws Exception {
        final File tempFolder = temp.newFolder();
        final List<ByteBuffer> recv = new ArrayList<>();
        final SlowStartEventQueue queue = new SlowStartEventQueue("apple",
                                                                  tempFolder.toPath(),
                                                                  executor,
                                                                  recv::add);

        queue.publish(createPayload(1));
        queue.publish(createPayload(2));
        queue.publish(createPayload(3));
        queue.ready();
        queue.publish(createPayload(4));
        queue.publish(createPayload(5));
        Thread.sleep(100);
        queue.publish(createPayload(6));

        while (recv.size() != 6) {
            Thread.sleep(100);
        }

        IntStream.range(0, 6)
                .forEachOrdered(i -> assertLongInBuf(i+1, recv.get(i)));
    }

    @Test(timeout = 30000)
    public void testStress() throws Exception {
        final File tempFolder = temp.newFolder();
        final List<ByteBuffer> recv = new ArrayList<>();
        final SlowStartEventQueue queue = new SlowStartEventQueue("orange",
                                                                  tempFolder.toPath(),
                                                                  executor,
                                                                  recv::add);

        final int preReady = 500000;
        final int postReady = 100000;
        final int postDrain = 100;
        final int expectedTotal = preReady+postReady+postDrain;
        for (int i = 0; i < preReady; i++) {
            queue.publish(createPayload(i+1));
        }
        queue.ready();
        Thread.sleep(10); // force a definite overlap of partial drain and insert to slow store
        for (int i = 0; i < postReady; i++) {
            queue.publish(createPayload(i+1+preReady));
        }

        Thread.sleep(100);
        for (int i = 0; i < postDrain; i++) {
            Thread.sleep(10);
            queue.publish(createPayload(i+1+preReady+postReady));
        }

        while (recv.size() != expectedTotal) {
            Thread.sleep(100);
        }
        final long timeToDrainNS = queue.getStats().getTimeToDrainNS();
        final float drainSec = timeToDrainNS / 1e9f;
        System.out.printf("Time to drain %dns or %fsec at %f msg/sec%n",
                          timeToDrainNS, drainSec, recv.size()/drainSec);

        for (int i = 0; i < expectedTotal; i++) {
            assertLongInBuf(i+1, recv.get(i));
        }

        assertThat(queue.getStats().getPreReady(), not(equalTo(0)));
        assertThat(queue.getStats().getPreDrained(), not(equalTo(0)));
        assertTrue(queue.getStats().getPreDrained() > queue.getStats().getPreReady());
        assertThat(queue.getStats().getDrained(), not(equalTo(0)));
        assertThat(queue.getStats().getTotal(), equalTo(((long) expectedTotal)));
        assertTrue(queue.getStats().getTotal() > queue.getStats().getPreDrained());
    }

    @Test
    public void testReadyBeforePublish() throws Exception {
        final File tempFolder = temp.newFolder();
        final List<ByteBuffer> recv = new ArrayList<>();
        final SlowStartEventQueue queue = new SlowStartEventQueue("pear",
                                                                  tempFolder.toPath(),
                                                                  executor,
                                                                  recv::add);

        queue.ready();
        queue.publish(createPayload(2));

        assertEquals(1, recv.size());
        assertLongInBuf(2, recv.get(0));
    }

    private void assertLongInBuf(int expected, ByteBuffer bb) {
        assertEquals("Not enough content", 4, bb.remaining());
        final int val = bb.getInt();
        assertEquals(expected, val);
    }

    private ByteBuffer createPayload(int val) {
        final ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(val);
        bb.rewind();
        return bb;
    }
}