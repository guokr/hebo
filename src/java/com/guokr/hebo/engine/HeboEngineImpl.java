package com.guokr.hebo.engine;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.guokr.hebo.HeboEngine;
import com.guokr.hebo.HeboCallback;

public class HeboEngineImpl implements HeboEngine {

    private DB          dbmaker     = DBMaker.newFileDB(new File("data/hebodb")).closeOnJvmShutdown().make();

    private Set<String> hashmapKeys = new HashSet<String>();

    private String[] keysMatching(Set<String> set, String pattern) {
        pattern = pattern.replaceAll("\\*", ".*");
        List<String> result = new ArrayList<>();
        for (String k : set) {
            if (k.matches(pattern)) {
                result.add(k);
            }
        }

        String allMatchedKeys[] = new String[result.size()];
        result.toArray(allMatchedKeys);

        return allMatchedKeys;
    }

    @Override
    public void keys(HeboCallback callback, String pattern) {
        // TODO Auto-generated method stub

        Set<String> set = dbmaker.getAll().keySet();

        callback.stringList(keysMatching(set, pattern));

        callback.response();
    }

    @Override
    public void del(HeboCallback callback, String key) {
        // TODO Auto-generated method stub
        if (dbmaker.exists(key)) {
            dbmaker.delete(key);
            if (hashmapKeys.contains(key)) {
                hashmapKeys.remove(key);
            }
            callback.integerValue(1);
        } else {
            callback.integerValue(0);
        }
 
        callback.response();
    }

    @Override
    public void get(HeboCallback callback, String key) {
        // TODO Auto-generated method stub
        callback.stringValue((String) dbmaker.get(key));
        callback.response();
    }

    @Override
    public void smembers(HeboCallback callback, String key) {
        // TODO Auto-generated method stub
        if (dbmaker.exists(key)) {
            Set<String> set = (Set<String>) dbmaker.get(key);
            String[] allElements = new String[set.size()];
            set.toArray(allElements);
            callback.stringList(allElements);
        } else {
            callback.stringList(new String[0]);
        }
        callback.response();
    }
    @Override
    public void sismember(HeboCallback callback, String key, String value) {
        // TODO Auto-generated method stub
        if (dbmaker.exists(key)) {
            Set<String> set = (Set<String>) dbmaker.get(key);
            callback.integerValue(set.contains(value) ? 1 : 0);
        } else {
            callback.error("ERR wrong number of arguments for 'sismember' command");
        }
        callback.response();
    }

    @Override
    public void sadd(HeboCallback callback, String key, String value) {
        // TODO Auto-generated method stub
        Set<String> set = null;

        if (dbmaker.exists(key)) {
            set = (Set<String>) dbmaker.get(key);

        } else {
            set = dbmaker.createHashSet(key).makeOrGet();
        }

        callback.integerValue(set.add(value) ? 1 : 0);
        callback.response();
    }

    @Override
    public void srem(HeboCallback callback, String key) {
        // TODO Auto-generated method stub

        if (dbmaker.exists(key)) {
            callback.integerValue(0);
        } else {
            Set<String> set = (Set<String>) dbmaker.get(key);
            callback.integerValue(set.remove(key) ? 1 : 0);
        }

        callback.response();
    }

    private String[] setsCompare(Set<String> set1, Set<String> set2) {
        if (set2 != null) {
            SetView<String> setdiff = Sets.difference(set1, set2);
            String diffarr[] = new String[setdiff.size()];
            setdiff.toArray(diffarr);
            return diffarr;
        } else {
            String set1arr[] = new String[set1.size()];
            set1.toArray(set1arr);
            return set1arr;
        }

    }

    @Override
    public void sdiff(HeboCallback callback, String key1, String key2) {
        // TODO Auto-generated method stub

        Set<String> set1 = (Set<String>) dbmaker.get(key1);
        Set<String> set2 = (Set<String>) dbmaker.get(key2);

        callback.stringList(setsCompare(set1, set2));
        callback.response();
    }

    @Override
    public void sdiffstore(HeboCallback callback, String key1, String key2, String key3) {
        // TODO Auto-generated method stub

        Set<String> set1 = (Set<String>) dbmaker.get(key2);
        Set<String> set2 = (Set<String>) dbmaker.get(key3);

        String[] diffarr = setsCompare(set1, set2);

        Set<String> set = null;
        if (dbmaker.exists(key1)) {
            set = (Set<String>) dbmaker.get(key1);
        } else {
            set = dbmaker.createHashSet(key1).makeOrGet();
        }
        for (String element : diffarr) {
            set.add(element);
        }
        callback.integerValue(set.size());
        callback.response();

    }

    @Override
    public void spop(HeboCallback callback, String key) {
        // TODO Auto-generated method stub

        if (dbmaker.exists(key)) {
            Set<String> set = (Set<String>) dbmaker.get(key);

            Iterator<String> iterator = set.iterator();
            String value = null;
            if (iterator.hasNext()) {
                value = iterator.next();
                set.remove(value);
            } 
            callback.stringValue(value);
        } else {
            callback.stringValue(null);
        }

        callback.response();
    }

    @Override
    public void hdel(HeboCallback callback, String key, String hkey) {
        // TODO Auto-generated method stub

        if (dbmaker.exists(key)) {

            HTreeMap<String, String> map = (HTreeMap<String, String>) dbmaker.get(key);

            callback.integerValue(map.remove(hkey) == null ? 0 : 1);

        } else {
            callback.integerValue(0);
        }

        callback.response();
    }

    @Override
    public void hset(HeboCallback callback, String key, String hkey, String hvalue) {
        // TODO Auto-generated method stub

        HTreeMap<String, String> map = null;
        if (dbmaker.exists(key)) {
            map = (HTreeMap<String, String>) dbmaker.get(key);
        } else {
            map = dbmaker.createHashMap(key).make();
            hashmapKeys.add(key);
        }
        callback.integerValue(map.put(hkey, hvalue) == null ? 1 : 0);

        callback.response();
    }

    @Override
    public void hkeys(HeboCallback callback, String pattern) {
        // TODO Auto-generated method stub
        callback.stringList(keysMatching(hashmapKeys, pattern));
        callback.response();
    }

    @Override
    public void hgetall(HeboCallback callback, String key) {
        // TODO Auto-generated method stub
        if (dbmaker.exists(key)) {
            HTreeMap<String, String> map = (HTreeMap<String, String>)dbmaker.get(key);
            Set<String> keyset = map.keySet();
            String result[] = new String[map.size()*2];
            int idx=0;
            for(String k: keyset) {
                result[idx++]=k;
                result[idx++]=map.get(k);
            }
            callback.stringList(result);
        } else {
            callback.stringList(new String[0]);
        }
        callback.response();
    }
    @Override
    public void rpush(HeboCallback callback, String key, String value) {
        // TODO Auto-generated method stub
        HTreeMap<Integer, String> map = null;
        if (dbmaker.exists(key)) {
            map = (HTreeMap<Integer, String>) dbmaker.get(key);
        } else {
            map = dbmaker.createHashMap(key).make();
        }
        map.put(map.size(), value);
        
        callback.integerValue(map.size());

        callback.response();
    }

    @Override
    public void llen(HeboCallback callback, String key) {
        // TODO Auto-generated method stub
        if (dbmaker.exists(key)) {
            HTreeMap<Integer, String>  map = (HTreeMap<Integer, String>) dbmaker.get(key); 
            callback.integerValue(map.size());
        } else {
            callback.integerValue(0);
        }
        callback.response();
    }
}
