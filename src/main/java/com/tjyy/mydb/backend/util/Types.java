package com.tjyy.mydb.backend.util;

/**
 * @author: Tjyy
 * @date: 2024-03-03 14:39
 * @description: types
 */
public class Types {
    public static long addressToUid(int pgno, short offset){
        long u0 = (long) pgno;
        long u1 = (long) offset;
        return u0 << 32 | u1;
    }
}
