package com.guokr.hebo.command;

import com.guokr.hebo.HeboEngine;
import com.guokr.hebo.HeboCallback;
import com.guokr.hebo.HeboCommand;

public class Rpush extends HeboCommand {

    @Override
    public String signature() {
        return "ss";
    }

    @Override
    public void invoke(HeboEngine engine, String key, String value,HeboCallback callback) {
        engine.rpush(callback, key, value);
    }

}
