package com.tjyy.mydb.backend.dm.dataitem;

import com.tjyy.mydb.backend.common.SubArray;
import com.tjyy.mydb.backend.dm.DataManagerImpl;
import com.tjyy.mydb.backend.dm.page.Page;

import java.util.concurrent.locks.Lock;

/**
 * @author: Tjyy
 * @date: 2024-03-03 14:31
 * @description: data item impl
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 */
public class DataItemImpl implements DataItem{
    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    private SubArray raw;
    private byte[] oldRaw;
    private Lock rLock;
    private Lock wLock;
    private DataManagerImpl dataManager;
    private long uid;
    private Page page;

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page page, long uid, DataManagerImpl dataManager) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        this.uid = uid;
        this.page = page;
        this.dataManager = dataManager;
    }

    public boolean isValid(){
        return raw.raw[raw.start + OF_VALID] == (byte) 0;
    }

    /**
     * 返回 DataItem 中的数据项
     * @return
     */
    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start + OF_DATA, raw.end);
    }

    /**
     * 在修改之前需要调用 before() 方法
     * 将原有数据保存一个备份到 oldRaw 中
     */
    @Override
    public void before() {
        wLock.lock();
        page.setDirty(true);
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    /**
     * 撤销修改时，需要调用 unBefore() 方法
     */
    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();
        // 并不能证明没有其他页进行了修改
        // page.setDirty(false);
    }

    /**
     * 在修改完成后，调用 after() 方法。
     * @param xid
     */
    @Override
    public void after(long xid) {
        dataManager.logDataItem(xid, this);
        wLock.unlock();
    }

    /**
     * 调用 release() 方法，释放掉 DataItem 的缓存
     */
    @Override
    public void release() {
        dataManager.releaseDataItem(this);
    }

    /**
     * 写锁 加锁
     */
    @Override
    public void lock() {
        wLock.lock();
    }

    /**
     * 写锁 解锁
     */
    @Override
    public void unlock() {
        wLock.unlock();
    }

    /**
     * 读锁 加锁
     */
    @Override
    public void rLock() {
        rLock.lock();
    }

    /**
     * 读锁 解锁
     */
    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    /**
     * 返回当前 DataItem 所在的页
     * @return
     */
    @Override
    public Page page() {
        return page;
    }

    /**
     * 返回当前 DataItem 的数据项ID ： uid
     * @return
     */
    @Override
    public long getUid() {
        return uid;
    }

    /**
     * 返回未修改数据
     * @return
     */
    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    /**
     * 返回当前数据项中可修改的数据
     * @return
     */
    @Override
    public SubArray getRaw() {
        return raw;
    }
}
