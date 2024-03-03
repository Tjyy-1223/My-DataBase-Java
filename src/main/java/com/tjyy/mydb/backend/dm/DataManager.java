package com.tjyy.mydb.backend.dm;

import com.tjyy.mydb.backend.dm.dataitem.DataItem;
import com.tjyy.mydb.backend.dm.logger.Logger;
import com.tjyy.mydb.backend.dm.page.FirstPage;
import com.tjyy.mydb.backend.dm.pageCache.PageCache;
import com.tjyy.mydb.backend.tm.TransactionManager;

public interface DataManager {
    /**
     * 从 DataManager 中读取数据抽象
     * @param uid
     * @return
     * @throws Exception
     */
    DataItem read(long uid) throws Exception;

    /**
     * 将数据抽象写入到文件中
     * @param xid
     * @param data
     * @return
     * @throws Exception
     */
    long insert(long xid, byte[] data) throws Exception;

    /**
     * 关闭数据管理器
     */
    void close();

    /**
     * 创建一个 DataManager 管理器: 其中有三个重要组成部分：
     * pageCache, logger, transactionManager
     * @param path
     * @param memory
     * @param transactionManager
     * @return
     */
    public static DataManager create(String path, long memory, TransactionManager transactionManager){
        PageCache pageCache = PageCache.create(path, memory);
        Logger logger = Logger.create(path);

        DataManagerImpl dataManager = new DataManagerImpl(pageCache, logger, transactionManager);
        dataManager.initFirstPage();
        return dataManager;
    }

    /**
     * 打开一个 DataManager 管理器: 其中有三个重要组成部分：
     * pageCache, logger, transactionManager
     * @param path
     * @param memory
     * @param transactionManager
     * @return
     */
    public static DataManager open(String path, long memory, TransactionManager transactionManager){
        PageCache pageCache = PageCache.open(path, memory);
        Logger logger = Logger.open(path);
        DataManagerImpl dataManager = new DataManagerImpl(pageCache, logger, transactionManager);
        if (!dataManager.loadCheckFirstPage()){
            Recover.recover(transactionManager, logger, pageCache);
        }

        dataManager.fillPageIndex();
        FirstPage.setInitString(dataManager.firstPage);
        dataManager.pageCache.flushPage(dataManager.firstPage);
        return dataManager;
    }

}
