package com.guokr.hebo.command;

import com.guokr.hebo.HeboEngine;
import com.guokr.hebo.HeboCallback;
import com.guokr.hebo.HeboCommand;

public class Hkeys extends HeboCommand {

    @Override
    public String signature() {
        return "s";
    }

    @Override
    public void invoke(HeboEngine engine, String key, HeboCallback callback) {
        engine.hkeys(callback, key);
    }

}