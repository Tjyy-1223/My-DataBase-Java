package com.tjyy.mydb.backend.dm.pageCache;

import com.tjyy.mydb.backend.common.AbstractCache;
import com.tjyy.mydb.backend.dm.page.Page;
import com.tjyy.mydb.backend.dm.page.PageImpl;
import com.tjyy.mydb.common.Error;
import com.tjyy.mydb.backend.util.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author: Tjyy
 * @date: 2024-02-26 18:02
 * @description: cache for page
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache {
    private static final int MEM_MIN_LIM = 10;  // 最小缓存大小
    public static final String DB_SUFFIX = ".db";

    // 读取数据需要的变量
    private RandomAccessFile randomAccessFile;
    private FileChannel channel;
    private Lock fileLock;

    // 定义数据库中有多少页
    private AtomicInteger pageNumbers;

    // 初始化 Page-Cache
    public PageCacheImpl(RandomAccessFile file, FileChannel channel, int maxResource) {
        super(maxResource);
        if (maxResource < MEM_MIN_LIM){
            Panic.panic(Error.MemTooSmallException);
        }

        long length = 0;
        try{
            length = file.length();
        }catch (IOException e) {
            Panic.panic(e);
        }

        this.randomAccessFile = file;
        this.channel = channel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int) length / PAGE_SIZE);
    }


    /**
     * 当资源不在缓存时候的获取行为，
     * @param key
     * @return
     * @throws Exception
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;
        long offset = pageOffset((int) key);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            channel.position(offset);
            channel.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pgno, buf.array(), this);
    }


    /**
     * 当资源被驱逐时候的写回策略
     * @param obj
     */
    @Override
    protected void releaseForCache(Page obj) {
        if(obj.isDirty()){
            flush(obj);
            obj.setDirty(false);
        }
    }


    /**
     * 为数据库文件新建一页
     * @param initData
     * @return
     */
    @Override
    public int newPage(byte[] initData) {
        int newPageNumber = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(newPageNumber, initData, null);
        flushPage(pg);
        return newPageNumber;
    }


    /**
     * 从缓存中获取对应的页，利用抽象函数定义好的函数
     * @param pageNumber
     * @return
     * @throws Exception
     */
    @Override
    public Page getPage(int pageNumber) throws Exception {
        return get((long) pageNumber);
    }


    /**
     * 关闭缓存，并释放相关资源
     */
    @Override
    public void close() {
        super.close();
        try {
            randomAccessFile.close();
            channel.close();
        }catch (IOException e){
            Panic.panic(e);
        }
    }

    /**
     * 驱逐缓存中的page页面
     * @param page
     */
    @Override
    public void release(Page page) {
        super.release((long) page.getPageNumber());
    }

    /**
     * 设置 DB 文件中的最大页数
     * @param maxPgno
     */
    @Override
    public void truncateByPgno(int maxPgno) {
        long size = pageOffset(maxPgno);
        try{
            randomAccessFile.setLength(size);
        }catch (IOException e){
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    /**
     * 获取数据库中页的长度
     * @return
     */
    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    /**
     * 将页面写入到磁盘中
     * @param page
     */
    @Override
    public void flushPage(Page page) {
        flush(page);
    }

    /**
     * 将页面写入到磁盘中
     * @param page
     */
    private void flush(Page page){
        int pgno = page.getPageNumber();
        long offset = pageOffset(pgno);

        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(page.getData());
            channel.position(offset);
            channel.write(buf);
            channel.force(false);
        }catch (IOException e){
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    /**
     * 根据页面序号获取文件中的存储位置
     * @param pgno
     * @return
     */
    private static long pageOffset(int pgno){
        return (long) (pgno - 1) * PAGE_SIZE;
    }


}
