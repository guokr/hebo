package com.guokr.hebo.command;

import com.guokr.hebo.HeboEngine;
import com.guokr.hebo.HeboCallback;
import com.guokr.hebo.HeboCommand;

public class Sdiff extends HeboCommand {

    @Override
    public String signature() {
        return "ss";
    }

    @Override
    public void invoke(HeboEngine engine, String key1, String key2,HeboCallback callback) {
        engine.sdiff(callback, key1,key2);
    }

}