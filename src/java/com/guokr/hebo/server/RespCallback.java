package com.guokr.hebo.server;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

public class RespCallback {
    private final SelectionKey key;
    private final HeboServer    server;

    public RespCallback(SelectionKey key, HeboServer server) {
        this.key = key;
        this.server = server;
    }

    // maybe in another thread :worker thread
    public void run(ByteBuffer... buffers) {
        server.tryWrite(key, buffers);
    }
}
