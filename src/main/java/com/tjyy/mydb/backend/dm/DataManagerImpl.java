package com.tjyy.mydb.backend.dm;

import com.tjyy.mydb.backend.common.AbstractCache;
import com.tjyy.mydb.backend.dm.dataitem.DataItem;
import com.tjyy.mydb.backend.dm.dataitem.DataItemImpl;
import com.tjyy.mydb.backend.dm.logger.Logger;
import com.tjyy.mydb.backend.dm.page.CommonPage;
import com.tjyy.mydb.backend.dm.page.FirstPage;
import com.tjyy.mydb.backend.dm.page.Page;
import com.tjyy.mydb.backend.dm.pageCache.PageCache;
import com.tjyy.mydb.backend.dm.pageIndex.PageIndex;
import com.tjyy.mydb.backend.dm.pageIndex.PageInfo;
import com.tjyy.mydb.backend.tm.TransactionManager;
import com.tjyy.mydb.backend.util.Panic;
import com.tjyy.mydb.backend.util.Types;
import com.tjyy.mydb.common.Error;

/**
 * @author: Tjyy
 * @date: 2024-03-03 14:25
 * @description: data manager
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{
    TransactionManager transactionManager;
    PageCache pageCache;
    Logger logger;
    PageIndex pageIndex;
    Page firstPage;

    public DataManagerImpl(PageCache pageCache, Logger logger, TransactionManager transactionManager){
        super(0);
        this.pageCache = pageCache;
        this.logger = logger;
        this.transactionManager = transactionManager;
        this.pageIndex = new PageIndex();
    }

    /**
     * 从 DataManager 中读取数据抽象
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl dataItem = (DataItemImpl) super.get(uid);
        if (!dataItem.isValid()){
            dataItem.release();
            return null;
        }
        return dataItem;
    }


    /**
     * 将数据抽象写入到文件中
     * @param xid
     * @param data
     * @return
     * @throws Exception
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.warpDataItemRaw(data);
        if (raw.length > CommonPage.MAX_PAGE_DATA_SIZE){
            throw Error.DataTooLargeException;
        }

        PageInfo pageInfo = null;
        for (int i = 0; i < 5; i++) {
            pageInfo = pageIndex.select(raw.length);

            if (pageInfo != null){
                break;
            }else{
                // 新建一页并添加到pageIndex索引中
                int newPgno = pageCache.newPage(CommonPage.initCommonPage());
                pageIndex.add(newPgno, CommonPage.MAX_PAGE_DATA_SIZE);
            }
        }

        if (pageInfo == null){
            throw Error.DatabaseBusyException;
        }

        // 成功找到可以存放 raw 的页，那么就需要分析该页的页号以及剩余存储空间
        // 根据 PageIndex{ pgno, space } 可以从 pageCache 中获取对应的页
        Page page = null;
        int freeSpace = 0;
        try {
            page = pageCache.getPage(pageInfo.pgno);
            // 根据 xid, page, raw 生成对应的 insertLog 记录
            byte[] log = Recover.insertLog(xid, page, raw);
            logger.log(log);

            short offset = CommonPage.insertData(page, raw);
            page.release();
            return Types.addressToUid(pageInfo.pgno, offset);
        }finally {
            // 将取出的 page 重新插入到 pageIndex 中
            if (page != null){
                pageIndex.add(pageInfo.pgno, CommonPage.getFreeSpace(page));
            }else{
                pageIndex.add(pageInfo.pgno, freeSpace);
            }
        }
    }


    /**
     * 关闭数据管理器
     */
    @Override
    public void close() {
        super.close();
        logger.close();

        FirstPage.setCloseString(firstPage);
        firstPage.release();
        pageCache.close();
    }


    /**
     * 根据 xid 和 dataItem 实体对象生成为 update 日志
     * @param xid
     * @param dataItem
     */
    public void logDataItem(long xid, DataItem dataItem){
        byte[] log = Recover.updateLog(xid, dataItem);
        logger.log(log);
    }

    /**
     * 释放 dataItem 对象
     * @param dataItem
     */
    public void releaseDataItem(DataItem dataItem){
        super.release(dataItem.getUid());
    }


    /**
     * 当资源不在缓存时候的获取行为
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        // 可以根据 uid 的生成工作获取 offset 和 pgno
        // uid = pgno << 32 | offset;
        short offset = (short) (uid & ((1L << 16) -1));
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));

        Page page = pageCache.getPage(pgno);
        return DataItem.parseDataItem(page, offset, this);
    }

    /**
     * 当资源被驱逐时候的写回策略
     * @param dataItem
     */
    @Override
    protected void releaseForCache(DataItem dataItem) {
        dataItem.page().release();
    }


    /**
     * 在创建文件时初始化 firstPage
     */
    void initFirstPage(){
        int pgno = pageCache.newPage(CommonPage.initCommonPage());
        assert pgno == 1;
        try {
            firstPage = pageCache.getPage(pgno);
        }catch (Exception e){
            Panic.panic(e);
        }
        pageCache.flushPage(firstPage);
    }

    /**
     * 在打开已有文件时时读入 firstPage，并验证正确性
     */
    boolean loadCheckFirstPage(){
        try {
            firstPage = pageCache.getPage(1);
        }catch (Exception e){
            Panic.panic(e);
        }
        return FirstPage.checkFirstPage(firstPage);
    }

    /**
     * 根据各个页面的页号以及空闲空间，初始化 pageIndex
     */
    void fillPageIndex(){
        int pageNumber = pageCache.getPageNumber();
        for (int i = 2; i <= pageNumber; i++) {
            Page page = null;
            try {
                page = pageCache.getPage(i);
            }catch (Exception e){
                Panic.panic(e);
            }

            pageIndex.add(page.getPageNumber(), CommonPage.getFreeSpace(page));
            page.release();
        }
    }
}
