package com.guokr.hebo.command;

import com.guokr.hebo.HeboEngine;
import com.guokr.hebo.HeboCallback;
import com.guokr.hebo.HeboCommand;

public class Sdiffstore extends HeboCommand {

    @Override
    public String signature() {
        return "sss";
    }

    @Override
    public void invoke(HeboEngine engine, String key1, String key2, String key3, HeboCallback callback) {
        engine.sdiffstore(callback, key1, key2, key3);
    }

}