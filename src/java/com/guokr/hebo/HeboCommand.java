package com.guokr.hebo;

import com.guokr.hebo.errors.HeboCommandException;

public abstract class HeboCommand {

    public abstract String signature();

    public void invoke(HeboEngine engine, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, String arg2, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, int arg2, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, float arg2, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, String[] arg2, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, int[] arg2, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, float[] arg2, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, String arg2, String arg3, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, String arg2, int arg3, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, String arg2, float arg3, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, String arg2, String[] arg3, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, String arg2, int[] arg3, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, String arg2, float[] arg3, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, int arg2, String arg3, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, int arg2, int arg3, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, int arg2, float arg3, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, int arg2, String[] arg3, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, int arg2, int[] arg3, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, int arg2, float[] arg3, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, float arg2, String arg3, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, float arg2, int arg3, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, float arg2, float arg3, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, float arg2, String[] arg3, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, float arg2, int[] arg3, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

    public void invoke(HeboEngine engine, String arg1, float arg2, float[] arg3, HeboCallback callback) {
        throw new HeboCommandException("Wrong parameters passed in");
    }

}
