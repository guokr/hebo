package com.guokr.hebo.command;

import com.guokr.hebo.HeboEngine;
import com.guokr.hebo.HeboCallback;
import com.guokr.hebo.HeboCommand;

public class Hset extends HeboCommand {

    @Override
    public String signature() {
        return "sss";
    }

    @Override
    public void invoke(HeboEngine engine, String key, String hkey, String hvalue,HeboCallback callback) {
        engine.hset(callback, key, hkey, hvalue);
    }

}