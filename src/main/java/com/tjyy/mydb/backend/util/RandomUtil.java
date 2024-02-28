package com.tjyy.mydb.backend.util;

import java.security.SecureRandom;
import java.util.Random;

/**
 * @author: Tjyy
 * @date: 2024-02-27 21:41
 * @description:
 */
public class RandomUtil {
    /**
     * 生成一个长度为length的随机字节数组
     * @param length
     * @return
     */
    public static byte[] randomBytes(int length){
        Random r = new SecureRandom();
        byte[] buf  = new byte[length];
        r.nextBytes(buf);
        return buf;
    }
}
