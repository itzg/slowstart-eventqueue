package me.itzg.slowstart;

import java.nio.ByteBuffer;

public interface EventConsumer {

    void consume(String key, ByteBuffer payload);
}
