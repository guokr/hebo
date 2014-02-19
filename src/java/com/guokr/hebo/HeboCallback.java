package com.guokr.hebo;

import java.nio.ByteBuffer;

public abstract class HeboCallback {

    public ByteBuffer buffer;

    public void flip() {
        if (buffer != null) {
            buffer.flip();
        }
    }

    public void ok() {
        buffer = ByteBuffer.allocate(5);
        buffer.put(HeboUtils.PLUS);
        buffer.put(HeboUtils.OK);
        buffer.put(HeboUtils.CRLF);
    }

    public void ok(String msg) {
        byte[] bytes = HeboUtils.bytes(msg);
        buffer = ByteBuffer.allocate(bytes.length + 3);
        buffer.put(HeboUtils.PLUS);
        buffer.put(bytes);
        buffer.put(HeboUtils.CRLF);
    }

    public void error(String msg) {
        byte[] bytes = HeboUtils.bytes(msg);
        buffer = ByteBuffer.allocate(bytes.length + 3);
        buffer.put(HeboUtils.MINUS);
        buffer.put(bytes);
        buffer.put(HeboUtils.CRLF);
    }

    public void status(int code) {
        byte[] bytes = HeboUtils.bytes(code);
        buffer = ByteBuffer.allocate(bytes.length + 3);
        buffer.put(HeboUtils.COLON);
        buffer.put(bytes);
        buffer.put(HeboUtils.CRLF);
    }

    public void integerValue(int val) {
        byte[] bytes = HeboUtils.bytes(val);
        byte[] size = HeboUtils.size(bytes);
        buffer = ByteBuffer.allocate(bytes.length + size.length + 5);
        buffer.put(HeboUtils.DOLLAR);
        buffer.put(size);
        buffer.put(HeboUtils.CRLF);
        buffer.put(bytes);
        buffer.put(HeboUtils.CRLF);
    }

    public void integerList(int[] list) {
        byte[] size = HeboUtils.size(list.length);
        buffer = ByteBuffer.allocate(1024 * list.length + 1024);
        buffer.put(HeboUtils.STAR);
        buffer.put(size);
        buffer.put(HeboUtils.CRLF);
        for (int val : list) {
            byte[] bytes = HeboUtils.bytes(val);
            size = HeboUtils.size(bytes);
            buffer.put(HeboUtils.DOLLAR);
            buffer.put(size);
            buffer.put(HeboUtils.CRLF);
            buffer.put(bytes);
            buffer.put(HeboUtils.CRLF);
        }
    }

    public void floatValue(float val) {
        byte[] bytes = HeboUtils.bytes(val);
        byte[] size = HeboUtils.size(bytes);
        buffer = ByteBuffer.allocate(bytes.length + size.length + 5);
        buffer.put(HeboUtils.DOLLAR);
        buffer.put(size);
        buffer.put(HeboUtils.CRLF);
        buffer.put(bytes);
        buffer.put(HeboUtils.CRLF);
    }

    public void floatList(float[] list) {
        byte[] size = HeboUtils.size(list.length);
        buffer = ByteBuffer.allocate(1024 * list.length + 1024);
        buffer.put(HeboUtils.STAR);
        buffer.put(size);
        buffer.put(HeboUtils.CRLF);
        for (float val : list) {
            byte[] bytes = HeboUtils.bytes(val);
            size = HeboUtils.size(bytes);
            buffer.put(HeboUtils.DOLLAR);
            buffer.put(size);
            buffer.put(HeboUtils.CRLF);
            buffer.put(bytes);
            buffer.put(HeboUtils.CRLF);
        }
    }

    public void stringValue(String val) {
        if (val == null) {
            buffer = ByteBuffer.allocate(5);
            buffer.put(HeboUtils.DOLLAR);
            buffer.put(HeboUtils.bytes("-1"));
            buffer.put(HeboUtils.CRLF);
        } else {
            byte[] bytes = HeboUtils.bytes(val);
            byte[] size = HeboUtils.size(bytes);
            buffer = ByteBuffer.allocate(bytes.length + size.length + 5);
            buffer.put(HeboUtils.DOLLAR);
            buffer.put(size);
            buffer.put(HeboUtils.CRLF);
            buffer.put(bytes);
            buffer.put(HeboUtils.CRLF);
        }

    }

    public void stringList(String[] list) {
        byte[] size = HeboUtils.size(list.length);
        buffer = ByteBuffer.allocate(1024 * list.length + 1024);
        buffer.put(HeboUtils.STAR);
        buffer.put(size);
        buffer.put(HeboUtils.CRLF);
        for (String str : list) {
            byte[] bytes = HeboUtils.bytes(str);
            size = HeboUtils.size(bytes);
            buffer.put(HeboUtils.DOLLAR);
            buffer.put(size);
            buffer.put(HeboUtils.CRLF);
            buffer.put(bytes);
            buffer.put(HeboUtils.CRLF);
        }
    }

    public abstract void response();

}
