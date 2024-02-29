package com.tjyy.mydb.backend.dm.logger;

import com.tjyy.mydb.backend.util.Panic;
import com.tjyy.mydb.backend.util.Parser;
import com.tjyy.mydb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author: Tjyy
 * @date: 2024-02-28 22:01
 * @description: logger 日志记录
 */
public interface Logger {
    public static final String LOG_SUFFIX = ".log";

    /**
     * 向日志中添加记录
     * @param data
     */
    void log(byte[] data);


    /**
     * 在x的位置对文件进行截断
     * @param x
     * @throws Exception
     */
    void truncate(long x) throws Exception;


    /**
     * 不断地从文件中读取下一条日志，并将其中的 Data 解析出来并返回。
     * @return
     */
    byte[] next();


    /**
     *  将指针重置为Log开始的位置
     */
    void rewind();


    /**
     *  关闭Log文件
     */
    void close();


    /**
     * 根据路径创建Logger类
     * @param path
     * @return
     */
    public static Logger create(String path){
        File file = new File(path + LOG_SUFFIX);
        try {
            if(!file.createNewFile()){
                Panic.panic(Error.FieldNotFoundException);
            }
        }catch (Exception e){
            Panic.panic(e);
        }

        if (!file.canRead() || ! file.canWrite()){
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

        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));
        try {
            channel.position(0);
            channel.write(buf);
            channel.force(false);
        }catch (IOException e){
            Panic.panic(e);
        }

        return new LoggerImpl(randomAccessFile, channel, 0);
    }


    /**
     * 根据路径读取Logger类
     */
    public static Logger open(String path){
        File file = new File(path + LOG_SUFFIX);
        if (!file.exists()){
            Panic.panic(Error.FileNotExistsException);
        }
        if (!file.canRead() || ! file.canWrite()){
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

        LoggerImpl logger = new LoggerImpl(randomAccessFile, channel);
        logger.init();
        return logger;
    }
}
