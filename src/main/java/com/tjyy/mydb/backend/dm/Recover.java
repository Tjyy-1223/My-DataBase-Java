package com.tjyy.mydb.backend.dm;

import com.google.common.primitives.Bytes;
import com.tjyy.mydb.backend.common.SubArray;
import com.tjyy.mydb.backend.dm.logger.Logger;
import com.tjyy.mydb.backend.dm.page.CommonPage;
import com.tjyy.mydb.backend.dm.page.Page;
import com.tjyy.mydb.backend.dm.pageCache.PageCache;
import com.tjyy.mydb.backend.tm.TransactionManager;
import com.tjyy.mydb.backend.util.Panic;
import com.tjyy.mydb.backend.util.Parser;

import java.util.*;

/**
 * @author: Tjyy
 * @date: 2024-02-29 11:19
 * @description: recover
 */
public class Recover {
    private static final byte LOG_TYPE_INSERT = 0;
    // insertLog: [LogType] [XID] [Pgno] [Offset] [Raw]

    private static final byte LOG_TYPE_UPDATE = 1;
    // updateLog: [LogType] [XID] [UID] [OldRaw] [NewRaw]

    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class InsertLogInfo{
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo{
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    /**
     * 对logger中为成功执行的事务进行恢复
     * @param transactionManager
     * @param logger
     * @param pageCache
     */
    public static void recover(TransactionManager transactionManager, Logger logger, PageCache pageCache){
        System.out.println("Recovering...");

        logger.rewind();  // 重置 position 到数据区域开始的地方

        int maxPgno = 0;
        while (true){
            // 遍历每一个 log 日志， 其中 next() 取出的是数据部分
            byte[] log = logger.next();
            if (log == null) break;

            int pgno;
            if (isInsertLog(log)){
                InsertLogInfo logInfo = parseInertLog(log);
                pgno = logInfo.pgno;
            }else{
                UpdateLogInfo logInfo = parseUpdateLog(log);
                pgno = logInfo.pgno;
            }

            if (pgno > maxPgno) {
                maxPgno = pgno;
            }
        }

        if (maxPgno == 0){
            maxPgno = 1;
        }

        pageCache.truncateByPgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        redoTransactions(transactionManager, logger, pageCache);
        System.out.println("Redo Transaction Over.");

        undoTransactions(transactionManager, logger, pageCache);
        System.out.println("Undo Transaction Over.");

        System.out.println("Recovery Over.");
    }

    /**
     * 执行 redo 操作
     * @param transactionManager
     * @param logger
     * @param pageCache
     */
    private static void redoTransactions(TransactionManager transactionManager, Logger logger, PageCache pageCache){
        logger.rewind();
        while (true){
            byte[] log = logger.next();
            if (log == null)
                break;

            if (isInsertLog(log)){
                InsertLogInfo insertLogInfo = parseInertLog(log);
                long xid = insertLogInfo.xid;
                if (!transactionManager.isActive(xid)){
                    // 当前事务为非活跃状态 Redo
                    doInsertLog(pageCache, log, REDO);
                }
            }else {
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                long xid = updateLogInfo.xid;
                if (!transactionManager.isActive(xid)){
                    doUpdateLog(pageCache, log, REDO);
                }
            }
        }
    }


    /**
     * 执行 undo 操作
     * @param transactionManager
     * @param logger
     * @param pageCache
     */
    private static void undoTransactions(TransactionManager transactionManager, Logger logger, PageCache pageCache){
        Map<Long, List<byte[]>> logCache = new HashMap<>();

        logger.rewind();
        while (true){
            byte[] log = logger.next();
            if (log == null)
                break;

            if (isInsertLog(log)){
                InsertLogInfo insertLogInfo = parseInertLog(log);
                long xid = insertLogInfo.xid;
                if (transactionManager.isActive(xid)){
                    // 当前事务为活跃状态 Undo
                    if (!logCache.containsKey(xid)){
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }else{
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                long xid = updateLogInfo.xid;
                if (transactionManager.isActive(xid)){
                    // 当前事务为活跃状态 Undo
                    if (!logCache.containsKey(xid)){
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        // 对所有 active log 进行倒叙undo
        for (Map.Entry<Long, List<byte[]>> entry :logCache.entrySet()){
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size() - 1; i >= 0; i--){
                byte[] log = logs.get(i);

                if (isInsertLog(log)){
                    doInsertLog(pageCache, log, UNDO);
                }else {
                    doInsertLog(pageCache, log, UNDO);
                }
            }
            // 设置回滚
            transactionManager.abort(entry.getKey());
        }
    }



    /**
     * 判断Log是否为 insert log
     * @param log
     * @return
     */
    private static boolean isInsertLog(byte[] log){
        return log[0] == LOG_TYPE_INSERT;
    }

    // update log 相关工作
    // [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE + 1;
    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    /**
     * 根据 xid 和 dataItem 生成对应的 updateLog 记录
     * @param xid
     * @param dataItem
     * @return
     */
    public static byte[] updateLog(long xid, DataItem dataItem){
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(dataItem.getUid());
        byte[] oldRaw = dataItem.getOldRaw();

        SubArray raw = dataItem.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    /**
     * 根据传入的 log 记录解析出各个元素
     * @param log
     * @return
     */
    private static UpdateLogInfo parseUpdateLog(byte[] log){
        UpdateLogInfo updateLogInfo = new UpdateLogInfo();
        updateLogInfo.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));

        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        updateLogInfo.offset = (short) (uid & ((1L << 16) - 1));

        uid >>>= 32;
        updateLogInfo.pgno = (int)(uid & ((1L << 32) - 1));

        int length = (log.length - OF_UPDATE_RAW) / 2;
        updateLogInfo.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        updateLogInfo.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + length * 2);
        return updateLogInfo;
    }

    /**
     * 执行更新的log语句
     * @param pageCache
     * @param log
     * @param flag
     */
    private static void doUpdateLog(PageCache pageCache, byte[] log, int flag){
        int pgno;
        short offset;
        byte[] raw;

        if (flag == REDO){
            UpdateLogInfo updateLogInfo = parseUpdateLog(log);
            pgno = updateLogInfo.pgno;
            offset = updateLogInfo.offset;
            raw = updateLogInfo.newRaw;
        }else{
            UpdateLogInfo updateLogInfo = parseUpdateLog(log);
            pgno = updateLogInfo.pgno;
            offset = updateLogInfo.offset;
            raw = updateLogInfo.oldRaw;
        }

        Page page = null;
        try {
            page = pageCache.getPage(pgno);
        }catch (Exception e){
            Panic.panic(e);
        }

        try {
            CommonPage.recoverUpdate(page, raw, offset);
        }finally {
            page.release();
        }
    }


    // insert log 相关工作
    // [LogType] [XID] [Pgno] [Offset] [Raw]
    private static final int OF_INSERT_PGNO = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    /**
     * 根据 xid, page, raw 生成对应的 insertLog 记录
     * @param xid
     * @param page
     * @param raw
     * @return
     */
    public static byte[] insertLog(long xid, Page page, byte[] raw){
        byte[] logType = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(page.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(CommonPage.getOffset(page));
        return Bytes.concat(logType, xidRaw, pgnoRaw, offsetRaw, raw);
    }


    /**
     * 根据传入的 log 记录解析出各个元素
     * @param log
     * @return
     */
    private static InsertLogInfo parseInertLog(byte[] log){
        InsertLogInfo insertLogInfo = new InsertLogInfo();
        insertLogInfo.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        insertLogInfo.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        insertLogInfo.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        insertLogInfo.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return insertLogInfo;
    }


    /**
     * 执行插入的log语句
     * @param pageCache
     * @param log
     * @param flag
     */
    private static void doInsertLog(PageCache pageCache, byte[] log, int flag){
        InsertLogInfo insertLogInfo = parseInertLog(log);
        Page page = null;
        try {
            page = pageCache.getPage(insertLogInfo.pgno);
        }catch (Exception e){
            Panic.panic(e);
        }
        try {
            if (flag == UNDO){
                DataItem.setDataItemRawInvalid(insertLogInfo.raw);
            }
            CommonPage.recoverInsert(page, insertLogInfo.raw, insertLogInfo.offset);
        }finally {
            page.release();
        }
    }
}
