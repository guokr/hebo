package com.guokr.hebo.command;

import com.guokr.hebo.HeboEngine;
import com.guokr.hebo.HeboCallback;
import com.guokr.hebo.HeboCommand;

public class Ping extends HeboCommand {

    @Override
    public String signature() {
        return "";
    }

    @Override
    public void invoke(HeboEngine engine, HeboCallback callback) {
        callback.ok("PONG");
    }

}
