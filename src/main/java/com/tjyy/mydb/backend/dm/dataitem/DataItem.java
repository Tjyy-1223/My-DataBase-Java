package com.tjyy.mydb.backend.dm.dataitem;

import com.google.common.primitives.Bytes;
import com.tjyy.mydb.backend.common.SubArray;
import com.tjyy.mydb.backend.dm.DataManager;
import com.tjyy.mydb.backend.dm.DataManagerImpl;
import com.tjyy.mydb.backend.dm.page.Page;
import com.tjyy.mydb.backend.util.Parser;
import com.tjyy.mydb.backend.util.Types;

import java.lang.reflect.Type;
import java.util.Arrays;

/**
 * @author: Tjyy
 * @date: 2024-03-03 14:14
 * @description: data item
 */
public interface DataItem {
    /**
     * 返回 DataItem 中的数据项
     * @return
     */
    SubArray data();

    /**
     * 在修改之前需要调用 before() 方法
     * 将原有数据保存一个备份到 oldRaw 中
     */
    void before();

    /**
     * 撤销修改时，需要调用 unBefore() 方法
     */
    void unBefore();

    /**
     * 在修改完成后，调用 after() 方法。
     * @param xid
     */
    void after(long xid);

    /**
     * 调用 release() 方法，释放掉 DataItem 的缓存
     */
    void release();

    /**
     * 写锁 加锁
     */
    void lock();

    /**
     * 写锁 解锁
     */
    void unlock();

    /**
     * 读锁 加锁
     */
    void rLock();


    /**
     * 读锁 解锁
     */
    void rUnLock();

    /**
     * 返回当前 DataItem 的数据项ID ： uid
     * @return
     */
    Page page();

    /**
     * 返回当前 DataItem 的数据项ID ： uid
     * @return
     */
    long getUid();


    /**
     * 返回未修改数据
     * @return
     */
    byte[] getOldRaw();


    /**
     * 返回当前数据项中可修改的数据
     * @return
     */
    SubArray getRaw();

    /**
     * 根据传入的数据生成一个 DataItem 数据抽象
     * 其中 ValidFlag 占用 1 字节，标识了该 DataItem 是否有效。
     * 删除一个 DataItem，只需要简单地将其有效位设置为 0。DataSize 占用 2 字节，标识了后面 Data 的长度。
     * @param raw
     * @return
     */
    public static byte[] warpDataItemRaw(byte[] raw){
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, size, raw);
    }

    /**
     * 从页面的 offset 处解析数据抽象 DataItem
     * @param page
     * @param offset
     * @param dataManager
     * @return
     */
    public static DataItem parseDataItem(Page page, short offset, DataManagerImpl dataManager){
        byte[] raw = page.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE,
                offset + DataItemImpl.OF_DATA));
        short length = (short) (size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(page.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset + length),
                new byte[length], page, uid, dataManager);
    }

    /**
     * 设置 DataItem 为无效状态
     * 其中 ValidFlag 占用 1 字节，标识了该 DataItem 是否有效。
     * 删除一个 DataItem，只需要简单地将其有效位设置为 1。
     * @param raw
     */
    public static void setDataItemRawInvalid(byte[] raw){
        raw[DataItemImpl.OF_VALID] = (byte) 1;
    }
}
