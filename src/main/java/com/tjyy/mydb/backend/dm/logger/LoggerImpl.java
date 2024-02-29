package com.tjyy.mydb.backend.dm.logger;

import com.google.common.primitives.Bytes;
import com.tjyy.mydb.backend.util.Panic;
import com.tjyy.mydb.backend.util.Parser;
import com.tjyy.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author: Tjyy
 * @date: 2024-02-28 22:24
 * @description: logger-impl
 */
public class LoggerImpl implements Logger{
    // Log基本格式如下
    private static final int SEED = 13331;
    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;

    // Log文件用到的变量如下
    private RandomAccessFile randomAccessFile;
    private FileChannel channel;
    private Lock lock;

    private long position;  // 当前日志指针的位置
    private long fileSize;  // 以及初始化时的记录
    private int xCheckSum;

    public LoggerImpl(RandomAccessFile randomAccessFile, FileChannel channel) {
        this.randomAccessFile = randomAccessFile;
        this.channel = channel;
        lock = new ReentrantLock();
    }

    public LoggerImpl(RandomAccessFile randomAccessFile, FileChannel channel, int xCheckSum) {
        this.randomAccessFile = randomAccessFile;
        this.channel = channel;
        this.xCheckSum = xCheckSum;
        lock = new ReentrantLock();
    }

    /**
     * Log 文件初始化, 用于读取 Log 文件之后的初始属性
     */
     void init(){
        long size = 0;
        try{
            size = randomAccessFile.length();
        }catch (IOException e){
            Panic.panic(Error.BadLogFileException);
        }

        if(size < 4){
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(4);
        try {
            channel.position(0);
            channel.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }

        int xCheckSum = Parser.parseInt(buf.array());
        this.fileSize = size;
        this.xCheckSum = xCheckSum;

        checkAndRemoveTail();
    }


    /**
     * 检查并移除上一次宕机导致的bad tail
     */
    private void checkAndRemoveTail(){
        rewind();

        int xCheck = 0;
        while (true){
            // 累计相加各个Log的累计和,对所有日志求出校验和，求和就能得到日志文件的校验和了。
            byte[] log = internNext();
            if (log == null)
                break;
            xCheck = calChecksum(xCheck, log);
        }
        if (xCheck != xCheckSum){
            Panic.panic(Error.BadLogFileException);
        }

        try {
            truncate(position);
        }catch (Exception e){
            Panic.panic(e);
        }

        try {
            randomAccessFile.seek(position);
        }catch (IOException e){
            Panic.panic(e);
        }
        rewind();
    }

    /**
     * 单条日志的校验和，其实就是通过一个指定的种子实现的
     * @param xCheck
     * @param log
     * @return
     */
    private int calChecksum(int xCheck, byte[] log){
        for (byte b :log){
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /**
     * 向日志中添加操作数据 data
     * 向日志文件写入日志时，也是首先将数据包裹成日志格式，
     * 写入文件后，再更新文件的校验和，更新校验和时，会刷新缓冲区，保证内容写入磁盘。
     * @param data
     */
    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();

        try {
            channel.position(channel.size());
            channel.write(buf);
        }catch (IOException e){
            Panic.panic(e);
        }finally {
            lock.unlock();
        }
        updateXChecksum(log);
    }

    private byte[] wrapLog(byte[] data){
        byte[] checkSum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checkSum, data);
    }

    private void updateXChecksum(byte[] log){
        this.xCheckSum = calChecksum(this.xCheckSum, log);
        try {
            channel.position(0);
            channel.write(ByteBuffer.wrap(Parser.int2Byte(xCheckSum)));
            channel.force(false);
        }catch (IOException e){
            Panic.panic(e);
        }
    }


    /**
     * 在x的位置对文件进行截断
     * @param x
     * @throws Exception
     */
    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            channel.truncate(x);
        }finally {
            lock.unlock();
        }
    }



    /**
     * 不断地从文件中读取下一条日志，并将其中的 Data 解析出来并返回。
     * @return
     */
    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null)
                return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        }finally {
            lock.unlock();
        }
    }

    private byte[] internNext(){
        if (position + OF_DATA >= fileSize){
            return null;
        }

        // 首先读取Log的长度
        ByteBuffer buf = ByteBuffer.allocate(4);
        try {
            channel.position(position);
            channel.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        int size = Parser.parseInt(buf.array());

        // 超出长度证明当前Log不是完整的
        if(position + size + OF_DATA > fileSize){
            return null;
        }

        // 读取checksum+data
        buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            channel.position(position);
            channel.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        byte[] log = buf.array();

        // 校验 checksum
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if (checkSum1 != checkSum2){
            return null;
        }

        position += log.length;
        return log;
    }


    /**
     * 将指针重置为Log开始的位置
     */
    @Override
    public void rewind() {
        position = 4;
    }

    /**
     * 关闭Log文件
     */
    @Override
    public void close() {
        try {
            channel.close();
            randomAccessFile.close();
        } catch(IOException e) {
            Panic.panic(e);
        }
    }
}
