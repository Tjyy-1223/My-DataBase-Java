package com.tjyy.mydb.backend.util;

import com.google.common.primitives.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author: Tjyy
 * @date: 2024-02-25 20:36
 * @description: parser
 */
public class Parser {
    public static byte[] short2Byte(short value){
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }

    public static short parseShort(byte[] buf){
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 2);
        return buffer.getShort();
    }

    public static byte[] int2Byte(int value) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }

    public static int parseInt(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 4);
        return buffer.getInt();
    }

    public static byte[] long2Byte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

    public static long parseLong(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        return buffer.getLong();
    }

    public static byte[] string2Byte(String str){
        byte[] array = int2Byte(str.length());
        return Bytes.concat(array, str.getBytes());
    }

    public static ParseStringRes parseString(byte[] buf){
        int length = parseInt(Arrays.copyOf(buf, 4));
        String str = new String(Arrays.copyOfRange(buf, 4, 4 + length));
        return new ParseStringRes(str, length + 4);
    }

    public static long str2Uid(String key){
        long seed = 13331;
        long res = 0;
        for (byte b : key.getBytes()){
            res = res * seed + (long)b;
        }
        return res;
    }


}
