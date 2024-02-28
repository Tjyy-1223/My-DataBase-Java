package com.tjyy.mydb.backend.dm.page;
import com.tjyy.mydb.backend.dm.pageCache.PageCache;
import com.tjyy.mydb.backend.util.Parser;

import java.util.Arrays;

/**
 * @author: Tjyy
 * @date: 2024-02-28 09:19
 * @description: 普通页结构
 *  [FreeSpaceOffset] [Data]
 *  FreeSpaceOffset: 2字节 空闲位置开始偏移
 */
public class CommonPage {
    private static final short OFFSET_FREE = 0;
    private static final short OFFSET_DATA = 2;
    public static final int MAX_PAGE_DATA_SIZE = PageCache.PAGE_SIZE - OFFSET_DATA;

    /**
     * 初始化普通页并返回数据
     * @return
     */
    public static byte[] initCommonPage(){
        byte[] data = new byte[PageCache.PAGE_SIZE];
        setOffset(data, OFFSET_DATA);
        return data;
    }

    /**
     * 为页面设置偏移量
     * @param page
     */
    public static void setOffset(Page page, short offset){
        setOffset(page.getData(), offset);
    }

    /**
     * 为页面数组设置偏移量
     * @param data
     * @param offset
     */
    private static void setOffset(byte[] data, short offset){
        System.arraycopy(Parser.short2Byte(offset), 0, data, OFFSET_FREE, OFFSET_DATA);
    }


    /**
     * 获得页面数据偏移量
     * @param page
     * @return
     */
    public static short getOffset(Page page){
        return getOffset(page.getData());
    }

    private static short getOffset(byte[] data){
        return Parser.parseShort(Arrays.copyOfRange(data, OFFSET_FREE, OFFSET_DATA));
    }


    /**
     * 向当前页面中插入数据,返回插入位置
     * @param data
     * @return
     */
    public static short insertData(Page page, byte[] data){
        page.setDirty(true);
        short offset = getOffset(page);
        System.arraycopy(data, 0 , page.getData(), offset, data.length);
        setOffset(page.getData(), (short) (offset + data.length));
        return offset;
    }

    /**
     * 获取空闲页面大小
     * @param page
     * @return
     */
    public static int getFreeSpace(Page page){
        return PageCache.PAGE_SIZE - (int)getOffset(page.getData());
    }

    /**
     * 在数据库崩溃后重新打开时，恢复例程
     * 向当前页面中 offset 插入数据,返回插入位置
     * @param page
     * @param data
     * @param offset
     * @return
     */
    public static short recoverInsert(Page page, byte[] data, short offset){
        page.setDirty(true);
        System.arraycopy(data, 0, page.getData(), offset, data.length);

        short originOffset = getOffset(page.getData());
        if(originOffset < offset + data.length){
            setOffset(page.getData(), (short) (offset + data.length));
        }
        return offset;
    }

    /**
     * 在数据库崩溃后重新打开时，恢复例程
     * 向当前页面中 offset 插入数据, 并覆盖其之后原有数据
     * @param page
     * @param data
     * @param offset
     * @return
     */
    public static short recoverUpdate(Page page, byte[] data, short offset){
        page.setDirty(true);
        System.arraycopy(data, 0, page.getData(), offset, data.length);
        return offset;
    }


}
