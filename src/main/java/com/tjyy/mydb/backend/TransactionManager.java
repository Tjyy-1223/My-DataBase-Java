package com.tjyy.mydb.backend;

import com.tjyy.mydb.common.Error;
import com.tjyy.mydb.util.Panic;
import jdk.internal.org.objectweb.asm.tree.FrameNode;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface TransactionManager {
    /**
     * 开启一个新事务
     * @return
     */
    long begin();

    /**
     * 提交一个事务
     */
    void commit(long xid);

    /**
     * 终止事务
     */
    void abort(long xid);

    /**
     * 查询一个状态的事务是否为活跃状态
     * @param xid
     * @return
     */
    boolean isActive(long xid);

    /**
     * 查询一个状态的事务是否为已提交状态
     * @param xid
     * @return
     */
    boolean isCommitted(long xid);

    /**
     * 查询一个状态的事务是否为取消
     * @param xid
     * @return
     */
    boolean isAborted(long xid);

    /**
     * 关闭 TM
     */
    void close();

    /**
     * 创建一个 xid 文件并创建 TM
     * @param path
     * @return
     */
    public static TransactionManagerImpl create(String path){
        File file = new File(path + TransactionManagerImpl.XID_SUFFER);
        try {
            if(!file.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
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

        // 写空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            channel.position(0);
            channel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(randomAccessFile, channel);
    }

    /**
     * 从一个已有的 xid 文件来创建 TM
     * @param path
     * @return
     */
    public static TransactionManagerImpl open(String path){
        File file = new File(path + TransactionManagerImpl.XID_SUFFER);

        if (!file.exists()){
            Panic.panic(Error.FileNotExistsException);
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
        return new TransactionManagerImpl(randomAccessFile, channel);
    }
}
