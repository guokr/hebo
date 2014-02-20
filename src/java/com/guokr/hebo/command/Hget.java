package com.guokr.hebo.command;

import com.guokr.hebo.HeboCallback;
import com.guokr.hebo.HeboCommand;
import com.guokr.hebo.HeboEngine;

public class Hget extends HeboCommand {

    @Override
    public String signature() {
        return "ss";
    }

    @Override
    public void invoke(HeboEngine engine, String key, String hkey, HeboCallback callback) {
        engine.hget(callback, key, hkey);
    }

}
