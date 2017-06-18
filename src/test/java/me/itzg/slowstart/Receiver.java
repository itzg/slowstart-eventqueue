package me.itzg.slowstart;

import org.junit.Assert;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by geoff on 6/18/17.
 */
public class Receiver implements EventConsumer {
    final List<ByteBuffer> recv = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void consume(String key, ByteBuffer payload) {
        recv.add(payload);
    }

    public int size() {
        return recv.size();
    }

    public ByteBuffer get(int i) {
        return recv.get(i);
    }

    public void waitFor(int eventCount) throws InterruptedException {
        while (recv.size() < eventCount) {
            Thread.sleep(10);
        }
    }

    public void assertContains(int... expectedValuesInThisOrder) {
        Iterator<ByteBuffer> itr = recv.iterator();
        int lookingFor = 0;

        while (itr.hasNext()) {
            ByteBuffer bb = itr.next();
            assertEquals(4, bb.remaining());
            int val = bb.getInt();
            bb.rewind();

            if (val == expectedValuesInThisOrder[lookingFor]) {
                if (++lookingFor >= expectedValuesInThisOrder.length) {
                    return;
                }
            }
        }

        Assert.fail(String.format("Expected value at %d was absent", lookingFor));
    }
}
