package com.tjyy.mydb.backend.dm.page;

/**
 * @author: Tjyy
 * @date: 2024-02-26 17:38
 * @description: page
 */
public interface Page {
    void lock();

    void unlock();

    void release();

    void setDirty(boolean dirty);

    boolean isDirty();

    int getPageNumber();

    byte[] getData();
}
