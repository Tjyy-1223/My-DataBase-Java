package com.tjyy.mydb.backend.dm.pageIndex;

import com.tjyy.mydb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author: Tjyy
 * @date: 2024-03-03 13:49
 * @description: page index
 */
public class PageIndex {
    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex(){
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];
        for (int i = 0; i < INTERVALS_NO + 1; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    /**
     * 将页面和剩余空间添加到 lists 中
     * @param pgno
     * @param freeSpace
     */
    public void add(int pgno, int freeSpace){
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pgno, freeSpace));
        }finally {
            lock.unlock();
        }
    }

    /**
     * 为需要 spaceSize 空间大小的数据寻找一个合适的页
     * @param spaceSize
     * @return
     */
    public PageInfo select(int spaceSize){
        lock.lock();
        try {
            int number = spaceSize / THRESHOLD;
            if (number < INTERVALS_NO)
                number++;
            while (number <= INTERVALS_NO){
                if (lists[number].size() == 0){
                    number++;
                    continue;
                }
                return lists[number].remove(0);
            }
            return null;
        }finally {
            lock.unlock();
        }
    }

}
