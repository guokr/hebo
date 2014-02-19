package com.guokr.hebo.server;

public class RedisAtta extends ServerAtta {

    public final RedisDecoder decoder;

    public RedisRequests      requests;

    public RedisAtta() {
        decoder = new RedisDecoder();
    }
}
