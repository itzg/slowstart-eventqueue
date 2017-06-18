package me.itzg.slowstart;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * Created by geoff on 6/18/17.
 */
public class TestUtils {
    static void assertIntInBuf(int expected, ByteBuffer bb) {
        assertEquals("Not enough content", 4, bb.remaining());
        final int val = bb.getInt();
        assertEquals(expected, val);
    }

    static ByteBuffer createPayload(int val) {
        final ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(val);
        bb.rewind();
        return bb;
    }
}
