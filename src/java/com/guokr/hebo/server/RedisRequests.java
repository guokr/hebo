package com.guokr.hebo.server;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.guokr.hebo.HeboRequest;
import com.guokr.hebo.server.RedisDecoder.State;

public class RedisRequests implements Iterable<HeboRequest> {

    public AsyncChannel      channel;
    public InetSocketAddress remoteAddr;
    public boolean           isFinished;

    private List<HeboRequest> list = new ArrayList<HeboRequest>();

    private HeboRequest       last;

    public State             state;
    public int               nargs;
    public int               nbytes;
    public String            line;
    public LineReader        lineReader;

    public RedisRequests() {
    }

    public void request(int size) {
        last = new HeboRequest(size);
        list.add(last);
    }

    public void add(String s) {
        last.add(s);
    }

    public int length() {
        return list.size();
    }

    @Override
    public Iterator<HeboRequest> iterator() {
        return list.iterator();
    }

    public void reset() {
        list.clear();
    }

}
