package com.tjyy.mydb.backend.dm.page;

import com.tjyy.mydb.backend.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author: Tjyy
 * @date: 2024-02-26 17:50
 * @description: page-impl
 */
public class PageImpl implements Page{
    private int pageNumber;  // 页号 从1开始增长
    private byte[] data;  // 数据
    private boolean dirty;  // 脏页判断
    private Lock lock;  // 页面锁
    private PageCache pageCache; // 页面缓存

    public PageImpl(int pageNumber, byte[] data, PageCache pageCache) {
        this.pageNumber = pageNumber;
        this.data = new byte[PageCache.PAGE_SIZE];
        System.arraycopy(data, 0, this.data, 0, data.length);

        this.pageCache = pageCache;
        lock = new ReentrantLock();
        dirty = false;
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {
        pageCache.release(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public int getPageNumber() {
        return pageNumber;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}
