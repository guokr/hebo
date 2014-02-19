package com.guokr.hebo;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.guokr.hebo.command.Del;
import com.guokr.hebo.command.Get;
import com.guokr.hebo.command.Hdel;
import com.guokr.hebo.command.Hgetall;
import com.guokr.hebo.command.Hkeys;
import com.guokr.hebo.command.Hset;
import com.guokr.hebo.command.Keys;
import com.guokr.hebo.command.Llen;
import com.guokr.hebo.command.Ping;
import com.guokr.hebo.command.Rpush;
import com.guokr.hebo.command.Sadd;
import com.guokr.hebo.command.Sdiff;
import com.guokr.hebo.command.Sdiffstore;
import com.guokr.hebo.command.Sismember;
import com.guokr.hebo.command.Smembers;
import com.guokr.hebo.command.Spop;
import com.guokr.hebo.command.Srem;
import com.guokr.hebo.engine.HeboEngineImpl;
import com.guokr.hebo.server.ServerHandler;
import com.guokr.hebo.server.HeboServer;

public class Hebo {

    private HeboServer server;

    public Hebo() throws IOException {
        HeboEngine engine = new HeboEngineImpl( );
        HeboRegistry registry = new HeboRegistry();

        registry.add("ping", new Ping());
        registry.add("del", new Del());
        registry.add("get", new Get());
        registry.add("hdel", new Hdel());
        registry.add("hgetall", new Hgetall());
        registry.add("hkeys", new Hkeys());
        registry.add("hset", new Hset());
        registry.add("keys", new Keys());
        registry.add("llen", new Llen());
        registry.add("rpush", new Rpush());
        registry.add("sadd", new Sadd());
        registry.add("sdiff", new Sdiff());
        registry.add("sdiffstore", new Sdiffstore());
        registry.add("sismember", new Sismember());
        registry.add("smembers", new Smembers());
        registry.add("spop", new Spop());
        registry.add("srem", new Srem());
        

        int port = 9876;
        
        String sport = System.getenv("HEBO_PORT");
        if (sport != null) {
            port = Integer.parseInt(sport);
        }
        server = new HeboServer("0.0.0.0", port,new ServerHandler(32, "simbase", 100, registry, engine));

    }

    public void run() throws IOException {
        server.start();
    }

    public static final void main(String[] args) {
        try {
            Hebo database = new Hebo();
            database.run();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
