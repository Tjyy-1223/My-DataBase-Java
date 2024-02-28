package com.tjyy.mydb.dm.page;

import com.tjyy.mydb.backend.dm.page.CommonPage;
import com.tjyy.mydb.backend.dm.page.FirstPage;
import com.tjyy.mydb.backend.dm.page.Page;
import com.tjyy.mydb.backend.dm.pageCache.PageCache;
import com.tjyy.mydb.backend.util.Parser;
import org.junit.Test;

/**
 * @author: Tjyy
 * @date: 2024-02-28 13:20
 * @description:
 */
public class TestCommonPage {
    private static final String path = "database";
    private static final long memory = 1 << 13 << 4;

    /**
     * 打开一个DB文件
     */
    @Test
    public void TestCommonPage() throws Exception {
        PageCache pageCache = PageCache.open(path, memory);
        System.out.println("打开了 DB 文件，并为其创建了缓存");

        int pageNumber = pageCache.getPageNumber();
        System.out.println("当前 数据库 的页面数量 " + pageNumber);

        byte[] data = CommonPage.initCommonPage();
        pageCache.newPage(data);

        int pgno = pageCache.getPageNumber();
        Page page = pageCache.getPage(pgno);
        System.out.println("当前页面为第 " + page.getPageNumber() + "页");
        System.out.println("当前页面offset为 " + CommonPage.getOffset(page));
        System.out.println("当前页面free space为 " + CommonPage.getFreeSpace(page));

        byte[] insertData = Parser.int2Byte(15);
        CommonPage.insertData(page, insertData);
        System.out.println("当前页面offset为 " + CommonPage.getOffset(page));
        System.out.println("当前页面free space为 " + CommonPage.getFreeSpace(page));
    }
}
