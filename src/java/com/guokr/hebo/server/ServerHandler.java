package com.guokr.hebo.server;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.guokr.hebo.HeboEngine;
import com.guokr.hebo.HeboCallback;
import com.guokr.hebo.HeboCommand;
import com.guokr.hebo.HeboRegistry;
import com.guokr.hebo.HeboRequest;
import com.guokr.hebo.HeboUtils;
import com.guokr.hebo.util.PrefixThreadFactory;

public class ServerHandler implements IHandler {
    private final ExecutorService execs;
    private final HeboRegistry     registry;
    private final HeboEngine       engine;

    public ServerHandler(int threadPoolSize, String prefix, int queueSize, HeboRegistry registry, HeboEngine engine) {
        this.registry = registry;
        this.engine = engine;
        PrefixThreadFactory factory = new PrefixThreadFactory(prefix);
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(queueSize);
        this.execs = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 0, TimeUnit.MILLISECONDS, queue, factory);
    }

    public void handle(RedisRequests reqs, final RespCallback cb) {
        try {
            for (final HeboRequest request : reqs) {
                execs.submit(new Runnable() {
                    @Override
                    public void run() {
                        HeboCallback callback = new ServerCallback(cb);
                        try {
                            HeboCommand command = registry.get(request.name().toLowerCase());
                            String sig = command.signature();
                            switch (sig.length()) {
                            case 0:
                                command.invoke(engine, callback);
                                break;
                            case 1:
                                command.invoke(engine, request.args(1), callback);
                                break;
                            case 2:
                                sig = sig.substring(1);
                                if (sig.equals("s")) {
                                    command.invoke(engine, request.args(1), request.args(2), callback);
                                } else if (sig.equals("i")) {
                                    command.invoke(engine, request.args(1), request.argi(2), callback);
                                } else if (sig.equals("f")) {
                                    command.invoke(engine, request.args(1), request.argf(2), callback);
                                } else if (sig.equals("S")) {
                                    command.invoke(engine, request.args(1), request.argS(2), callback);
                                } else if (sig.equals("I")) {
                                    command.invoke(engine, request.args(1), request.argI(2), callback);
                                } else if (sig.equals("F")) {
                                    command.invoke(engine, request.args(1), request.argF(2), callback);
                                }
                                break;
                            case 3:
                                sig = sig.substring(1);
                                if (sig.equals("ss")) {
                                    command.invoke(engine, request.args(1), request.args(2), request.args(3), callback);
                                } else if (sig.equals("si")) {
                                    command.invoke(engine, request.args(1), request.args(2), request.argi(3), callback);
                                } else if (sig.equals("sf")) {
                                    command.invoke(engine, request.args(1), request.args(2), request.argf(3), callback);
                                } else if (sig.equals("sS")) {
                                    command.invoke(engine, request.args(1), request.args(2), request.argS(3), callback);
                                } else if (sig.equals("sI")) {
                                    command.invoke(engine, request.args(1), request.args(2), request.argI(3), callback);
                                } else if (sig.equals("sF")) {
                                    command.invoke(engine, request.args(1), request.args(2), request.argF(3), callback);
                                } else if (sig.equals("is")) {
                                    command.invoke(engine, request.args(1), request.argi(2), request.args(3), callback);
                                } else if (sig.equals("ii")) {
                                    command.invoke(engine, request.args(1), request.argi(2), request.argi(3), callback);
                                } else if (sig.equals("if")) {
                                    command.invoke(engine, request.args(1), request.argi(2), request.argf(3), callback);
                                } else if (sig.equals("iS")) {
                                    command.invoke(engine, request.args(1), request.argi(2), request.argS(3), callback);
                                } else if (sig.equals("iI")) {
                                    command.invoke(engine, request.args(1), request.argi(2), request.argI(3), callback);
                                } else if (sig.equals("iF")) {
                                    command.invoke(engine, request.args(1), request.argi(2), request.argF(3), callback);
                                } else if (sig.equals("fs")) {
                                    command.invoke(engine, request.args(1), request.argf(2), request.args(3), callback);
                                } else if (sig.equals("fi")) {
                                    command.invoke(engine, request.args(1), request.argf(2), request.argi(3), callback);
                                } else if (sig.equals("ff")) {
                                    command.invoke(engine, request.args(1), request.argf(2), request.argf(3), callback);
                                } else if (sig.equals("fS")) {
                                    command.invoke(engine, request.args(1), request.argf(2), request.argS(3), callback);
                                } else if (sig.equals("fI")) {
                                    command.invoke(engine, request.args(1), request.argf(2), request.argI(3), callback);
                                }
                                break;
                            }
                        } catch (Exception e) {
                            if (e instanceof IndexOutOfBoundsException) {
                                callback.error(String.format("Invalid arguments for command '%s'", request.name()));
                            } else {
                                callback.error(e.getMessage());
                            }
                            callback.response();
                        }
                    }
                });
            }
        } catch (RejectedExecutionException e) {
            HeboUtils.printError("increase :queue-size if this happens often", e);
        }
    }

    public void close(int timeoutTs) {
        if (timeoutTs > 0) {
            execs.shutdown();
            try {
                if (!execs.awaitTermination(timeoutTs, TimeUnit.MILLISECONDS)) {
                    execs.shutdownNow();
                }
            } catch (InterruptedException ie) {
                execs.shutdownNow();
                Thread.currentThread().interrupt();
            }
        } else {
            execs.shutdownNow();
        }
    }

    public void clientClose(final AsyncChannel channel, final int status) {
        if (channel.closedRan == 0) { // server did not close it first
            // has close handler, execute it in another thread
            if (channel.closeHandler != null) {
                try {
                    // no need to maintain order
                    execs.submit(new Runnable() {
                        public void run() {
                            try {
                                channel.onClose(status);
                            } catch (Exception e) {
                                HeboUtils.printError("on close handler", e);
                            }
                        }
                    });
                } catch (RejectedExecutionException e) {
                    HeboUtils.printError("increase :queue-size if this happens often", e);
                }
            } else {
                // no close handler, mark the connection as closed
                // channel.closedRan = 1;
                // lazySet
                AsyncChannel.unsafe.putOrderedInt(channel, AsyncChannel.closedRanOffset, 1);
            }
        }
    }

}
