package com.guokr.hebo.errors;

public class HeboCommandException extends SimException {

    private static final long serialVersionUID = 8529211973276461863L;

    public HeboCommandException(String msg) {
        super(msg);
    }

    public HeboCommandException(Throwable t) {
        super(t);
    }

    public HeboCommandException(String msg, Throwable t) {
        super(msg, t);
    }
}
