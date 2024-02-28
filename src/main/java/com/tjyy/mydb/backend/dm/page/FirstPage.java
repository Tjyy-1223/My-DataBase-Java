package com.tjyy.mydb.backend.dm.page;

import com.tjyy.mydb.backend.dm.pageCache.PageCache;
import com.tjyy.mydb.backend.util.RandomUtil;

import java.util.Arrays;

/**
 * @author: Tjyy
 * @date: 2024-02-27 21:32
 * @description: first-page valid check,特殊管理，用于判断上一次数据库是否正常关闭
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 用于判断上一次数据库是否正常关闭
 */
public class FirstPage {
    private static final int startOffset = 100;
    private static final int validLength = 8;

    /**
     * 初始化第一页,用于当前DB中没有第一页的情况
     * @return
     */
    public static byte[] initFirstPage(){
        byte[] data = new byte[PageCache.PAGE_SIZE];
        setInitString(data);
        return data;
    }

    /**
     * 为已有第一页设置验证字符
     * @param page
     */
    public static void setInitString(Page page){
        page.setDirty(true);
        setInitString(page.getData());
    }

    /**
     * 为第一页添加验证字符
     * @param data
     */
    private static void setInitString(byte[] data){
        byte[] validString = RandomUtil.randomBytes(validLength);
        System.arraycopy(validString, 0, data, startOffset, validLength);
    }



    /**
     * 为已有第一页设置终止字符
     */
    public static void setCloseString(Page page){
        page.setDirty(true);
        setCloseString(page.getData());
    }



    /**
     * 为第一页添加终止字符
     * @param data
     */
    private static void setCloseString(byte[] data){
        System.arraycopy(data, startOffset, data, startOffset + validLength, validLength);
    }


    public static boolean checkFirstPage(Page page){
        return checkValidString(page.getData());
    }


    /**
     * 检验数组中的有效参数是否正确
     * @param data
     */
    private static boolean checkValidString(byte[] data){
        return Arrays.equals(Arrays.copyOfRange(data, startOffset, startOffset + validLength),
                Arrays.copyOfRange(data, startOffset+ validLength, startOffset + 2 * validLength));
    }
}
