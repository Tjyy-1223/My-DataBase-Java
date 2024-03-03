package com.tjyy.mydb.backend.dm.pageIndex;

/**
 * @author: Tjyy
 * @date: 2024-03-03 13:50
 * @description: page info
 */
public class PageInfo {
    public int pgno;
    public int freeSpace;

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}
