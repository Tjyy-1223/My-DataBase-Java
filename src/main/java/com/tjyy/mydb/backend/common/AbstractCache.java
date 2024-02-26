package com.tjyy.mydb.backend.common;

import com.sun.org.apache.xpath.internal.operations.Bool;
import com.tjyy.mydb.common.Error;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author: Tjyy
 * @date: 2024-02-26 14:54
 * @description: AbstractCache 实现了一个引用计数策略的缓存
 */
public abstract class AbstractCache<T>{
    private HashMap<Long, T> cache;  // 保存缓存数据
    private HashMap<Long, Integer> references; // 元素的引用个数
    private HashMap<Long, Boolean> getting; // 正在被获取的资源

    private int maxResource;  // 缓存的最大缓存资源数
    private int count = 0; // 缓存中当前的元素个数
    private Lock lock;

    public AbstractCache(int maxResource){
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 获取数据库资源
     * @param key
     * @return
     * @throws Exception
     */
    protected T get(long key) throws Exception{
        while (true){
            lock.lock();
            if (getting.containsKey(key)){
                // 请求的资源正在被其他线程获取
                lock.unlock();
                try {
                    Thread.sleep(1);
                }catch (InterruptedException e){
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            if (cache.containsKey(key)){
                // 资源在缓存中，可以直接返回
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }

            // 如果不再缓存中，尝试获取资源
            if(maxResource > 0 && count == maxResource){
                lock.unlock();
                throw Error.CacheFullException;
            }

            // 打破循环 不重复在缓存中获取
            count++;
            getting.put(key, true);
            lock.unlock();
            break;
        }

        // 调用当资源不在缓存时候的获取行为
        T obj = null;
        try {
            obj = getForCache(key);
        }catch (Exception e){
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();
        return obj;
    }

    /**
     * 强行释放一个缓存
     * @param key
     */
    protected void release(long key){
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            // 没有其他引用时，可以释放该资源
            if (ref == 0){
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count--;
            }else{
                references.put(key, ref);
            }
        }finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存 写回所有资源
     */
    protected void close(){
        lock.lock();
        try{
            Set<Long> keys = cache.keySet();
            for (Long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        }finally {
            lock.unlock();
        }
    }

    /**
     * 当资源不在缓存时候的获取行为
     * @param key
     * @return
     * @throws Exception
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时候的写回策略
     * @param onj
     */
    protected abstract void releaseForCache(T onj);
}
