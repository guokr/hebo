package com.guokr.hebo;

public interface HeboEngine {

    public void keys(HeboCallback callback, String key);

    public void del(HeboCallback callback, String key);

    public void get(HeboCallback callback, String key);

    public void set(HeboCallback callback, String key, String value);

    public void smembers(HeboCallback callback, String key);

    public void sismember(HeboCallback callback, String key, String value);

    public void sadd(HeboCallback callback, String key, String value);

    public void srem(HeboCallback callback, String key, String value);

    public void sdiff(HeboCallback callback, String key1, String key2);

    public void sdiffstore(HeboCallback callback, String key1, String key2, String key3);

    public void spop(HeboCallback callback, String key);

    public void hdel(HeboCallback callback, String key, String hkey);

    public void hset(HeboCallback callback, String key, String hkey, String hvalue);

    public void hkeys(HeboCallback callback, String key);

    public void hgetall(HeboCallback callback, String key);

    public void hget(HeboCallback callback, String key, String hkey);

    public void rpush(HeboCallback callback, String key, String value);

    public void llen(HeboCallback callback, String key);
}