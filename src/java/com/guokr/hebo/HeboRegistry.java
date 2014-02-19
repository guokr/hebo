package com.guokr.hebo;

import java.util.HashMap;
import java.util.Map;

import com.guokr.hebo.errors.HeboCommandException;

public class HeboRegistry {

    private Map<String, HeboCommand> registry = new HashMap<String, HeboCommand>();

    public void add(String key, HeboCommand cmd) {
        registry.put(key, cmd);
    }

    public HeboCommand get(String key) {
        HeboCommand command = registry.get(key);
        if (command == null) {
            throw new HeboCommandException(String.format("Unknown command '%s'", key));
        }
        return command;
    }
}
