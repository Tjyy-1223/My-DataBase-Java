package com.tjyy.mydb.dm.pageCache;

import com.tjyy.mydb.backend.dm.page.Page;
import com.tjyy.mydb.backend.dm.pageCache.PageCache;
import com.tjyy.mydb.backend.util.Parser;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author: Tjyy
 * @date: 2024-02-28 10:44
 * @description: test page cache
 */
public class TestPageCache {
    private static final String path = "database";
    private static final String path_fail = "database_fail";
    private static final long memory = 1 << 13 << 4;

    /**
     * 测试创建一个新的DB文件
     */
    @Test
    public void TestCreatePageCache(){
        PageCache pageCache = PageCache.create(path, memory);
        System.out.println("创建了新的 DB 文件，并为其创建了缓存");

        int pageNumber = pageCache.getPageNumber();
        System.out.println("当前 数据库 的页面数量 " + pageNumber);
    }


    /**
     * 打开一个DB文件
     */
    @Test
    public void TestOpenPageCache(){
        PageCache pageCache = PageCache.open(path, memory);
        System.out.println("打开了 DB 文件，并为其创建了缓存");

        int pageNumber = pageCache.getPageNumber();
        System.out.println("当前 数据库 的页面数量 " + pageNumber);
    }

    /**
     * 使用 PageCache 获取一页数据
     */
    @Test
    public void TestGetPage() throws Exception {
        PageCache pageCache = PageCache.open(path, memory);
        System.out.println("打开了 DB 文件，并为其创建了缓存");

        int pgno = 3;
        Page page = pageCache.getPage(pgno);
        System.out.println("当前页面为第 " + page.getPageNumber() + "页");
        System.out.println("当前页面数据长度为：" + page.getData().length);

        int pageNumber = pageCache.getPageNumber();
        System.out.println("当前 数据库 的页面数量 " + pageNumber);
    }

    /**
     * 新建一个页面
     */
    @Test
    public void TestCreateNewPage() throws Exception {
        PageCache pageCache = PageCache.open(path, memory);
        System.out.println("打开了 DB 文件，并为其创建了缓存");

        int pageNumber = pageCache.getPageNumber();
        System.out.println("当前 数据库 的页面数量 " + pageNumber);

        byte[] data = Parser.long2Byte(88956L);
        pageCache.newPage(data);
        pageNumber = pageCache.getPageNumber();
        System.out.println("当前 数据库 的页面数量 " + pageNumber);

        pageCache.newPage(data);
        pageNumber = pageCache.getPageNumber();
        System.out.println("当前 数据库 的页面数量 " + pageNumber);

        int pgno = 2;
        Page page = pageCache.getPage(pgno);
        System.out.println("当前页面为第 " + page.getPageNumber() + "页");
        // System.out.println("当前页面数据为：" + Arrays.toString(page.getData()));
        System.out.println(page.isDirty());

        pageCache.close();

        pageCache = PageCache.open(path, memory);
        System.out.println("打开了 DB 文件，并为其创建了缓存");
        pageNumber = pageCache.getPageNumber();
        System.out.println("当前 数据库 的页面数量 " + pageNumber);
    }

    /**
     * 测试获取页面数据
     */
    @Test
    public void TestReleasePage() throws Exception {
        PageCache pageCache = PageCache.open(path, memory);
        System.out.println("打开了 DB 文件，并为其创建了缓存");

        int pageNumber = pageCache.getPageNumber();
        System.out.println("当前 数据库 的页面数量 " + pageNumber);

        int pgno = 3;
        Page page = pageCache.getPage(pgno);
        pageCache.release(page);

        pageNumber = pageCache.getPageNumber();
        System.out.println("当前 数据库 的页面数量 " + pageNumber);

        // 顺便检测了 Page 功能的函数
        System.out.println(page.isDirty());
        page.setDirty(true);
        System.out.println(page.isDirty());

        // 再次释放会报错
        pageCache.release(page);
    }


}
