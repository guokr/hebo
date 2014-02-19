package com.guokr.hebo.errors;

public class HeboException extends SimException {

    private static final long serialVersionUID = 6830663864818521600L;

    public HeboException(String msg) {
        super(msg);
    }

    public HeboException(Throwable t) {
        super(t);
    }

    public HeboException(String msg, Throwable t) {
        super(msg, t);
    }
}
