package com.tjyy.mydb.backend.tm;

import com.tjyy.mydb.common.Error;
import com.tjyy.mydb.util.Panic;
import com.tjyy.mydb.util.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author: Tjyy
 * @date: 2024-02-25 20:11
 * @description: transaction manager
 */
public class TransactionManagerImpl implements TransactionManager{
    // XID 文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;
    // 每个事务占用长度
    private static final int XID_FIELD_SIZE = 1;

    // 事务的三种状态
    private static final byte FILE_TRAN_ACTIVE = 0;
    private static final byte FILE_TRAN_COMMITTED = 1;
    private static final byte FILE_TRAN_ABORTED = 2;

    // 超级事务
    public static final long SUPER_XID = 0;

    // XID文件后缀
    static final String XID_SUFFER = ".xid";

    private RandomAccessFile file;
    private FileChannel channel;
    private long xidCounter = 0;
    private Lock counterLock;

    TransactionManagerImpl(RandomAccessFile file, FileChannel channel){
        this.file = file;
        this.channel = channel;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 校验XID是否合法：与文件中的前8个字符以及文件长度进行比较
     * 读取XID_FILE_HEADER中的xidcounter，根据它计算文件的理论长度，对比实际长度
     */
    private void checkXIDCounter(){
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e) {
            Panic.panic(Error.BadXIDFileException);
        }

        if(fileLen < LEN_XID_HEADER_LENGTH){
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            channel.position(0);
            channel.read(buf);
        }catch (IOException e){
           Panic.panic(e);
        }

        // 获取事务控制长度 即当前存在的事务总数
        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXidPosition(this.xidCounter + 1);
        if(end != fileLen){
            Panic.panic(Error.BadXIDFileException);
        }
    }

    /**
     * 根据事务xid取得其在xid文件中对应的位置
     * @param xid
     * @return
     */
    private long getXidPosition(long xid){
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    /**
     * 更新xid事务的状态为status
     * @param xid
     * @param status
     */
    private void updateXidStatus(long xid, byte status){
        if(xid > xidCounter || xid < 0){
            Panic.panic(Error.BadXIDFileException);
        }

        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;

        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            channel.position(offset);
            channel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        try {
            channel.force(false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 将XID加一，并更新XID Header
     * 设置状态为活跃
     */
    private void incrXidCounter(){
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));

        // 更新当前事务数量
        try {
            channel.position(0);
            channel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 更新初始状态
        long pos = getXidPosition(xidCounter);
        byte[] arr = new byte[]{FILE_TRAN_ACTIVE};
        buf = ByteBuffer.wrap(arr);

        try {
            channel.position(pos);
            channel.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 更新后的数据刷出到磁盘
        try {
            channel.force(false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 开始一个事务，并返回XID
     * @return
     */
    @Override
    public long begin() {
        counterLock.lock();
        try{
            long xid = xidCounter + 1;
            incrXidCounter();
            return xid;
        }finally {
            counterLock.unlock();
        }
    }

    @Override
    public void commit(long xid) {
        updateXidStatus(xid, FILE_TRAN_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        updateXidStatus(xid, FILE_TRAN_ABORTED);
    }

    /**
     * 检测XID事务是否处于status状态
     * @param xid
     * @param status
     * @return
     */
    private boolean checkXidStatus(long xid, byte status){
        long pos = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.allocate(XID_FIELD_SIZE);
        try{
            channel.position(pos);
            channel.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }

        return buf.array()[0] == status;
    }

    @Override
    public boolean isActive(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXidStatus(xid, FILE_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if(xid == SUPER_XID) return true;
        return checkXidStatus(xid, FILE_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXidStatus(xid, FILE_TRAN_ABORTED);
    }

    @Override
    public void close() {
        try {
            channel.close();
            file.close();
        }catch (IOException e){
            Panic.panic(e);
        }
    }
}
