package com.guokr.hebo.server;

import com.guokr.hebo.HeboCallback;

public class ServerCallback extends HeboCallback {

    public RespCallback response;

    public ServerCallback(RespCallback cb) {
        this.response = cb;
    }

    public void response() {
        if (this.buffer == null) {
            this.error("Unknown server error!");
        }
        this.buffer.flip();
        this.response.run(this.buffer);
    }

}
