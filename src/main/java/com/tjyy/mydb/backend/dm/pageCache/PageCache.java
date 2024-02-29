package com.tjyy.mydb.backend.dm.pageCache;

import com.tjyy.mydb.backend.dm.page.Page;
import com.tjyy.mydb.common.Error;
import com.tjyy.mydb.backend.util.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * @author: Tjyy
 * @date: 2024-02-26 17:58
 * @description: page-cache
 */
public interface PageCache {
    public static int PAGE_SIZE = 1 << 13;

    /**
     * 根据给定的路径和内存空间大小创建DB文件并生成一个缓冲池实例
     * @param path
     * @param memory
     * @return
     */
    public static PageCacheImpl create(String path, long memory){
        File file = new File(path + PageCacheImpl.DB_SUFFIX);
        try{
            if (!file.createNewFile()){
                Panic.panic(Error.FileExistsException);
            }
        }catch (Exception e){
            Panic.panic(e);
        }
        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel channel = null;
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            channel = randomAccessFile.getChannel();
        }catch (FileNotFoundException e){
            Panic.panic(e);
        }

        return new PageCacheImpl(randomAccessFile, channel, (int)memory/PAGE_SIZE);
    }

    /**
     * 根据给定的路径和内存空间大小打开DB文件并读取一个缓冲池实例
     * @param path
     * @param memory
     * @return
     */
    public static PageCacheImpl open(String path, long memory){
        File file = new File(path + PageCacheImpl.DB_SUFFIX);
        try{
            if (!file.exists()){
                Panic.panic(Error.FileExistsException);
            }
        }catch (Exception e){
            Panic.panic(e);
        }
        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel channel = null;
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            channel = randomAccessFile.getChannel();
        }catch (FileNotFoundException e){
            Panic.panic(e);
        }
        return new PageCacheImpl(randomAccessFile, channel, (int)memory/PAGE_SIZE);
    }


    /**
     * 新建一个页
     * @param initData
     * @return
     */
    int newPage(byte[] initData);

    /**
     * 获取pageNumber开始的一页数据
     * @param pageNumber
     * @return
     * @throws Exception
     */
    Page getPage(int pageNumber) throws Exception;

    /**
     * 关闭缓存
     */
    void close();

    /**
     * 从缓存中移除页的内容
     * @param page
     */
    void release(Page page);

    /**
     * 设置 DB 文件中的最大页数
     * @param maxPgno
     */
    void truncateByPgno(int maxPgno);

    /**
     * 当前数据库中页面数量
     * @return
     */
    int getPageNumber();

    /**
     * 将页面内容刷入磁盘
     * @param page
     */
    void flushPage(Page page);
}
