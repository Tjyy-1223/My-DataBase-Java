package com.tjyy.mydb.backend.common;

/**
 * @author: Tjyy
 * @date: 2024-02-26 16:37
 * @description: sub-array
 */
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
