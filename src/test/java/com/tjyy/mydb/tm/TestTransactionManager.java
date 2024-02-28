package com.tjyy.mydb.tm;

import com.tjyy.mydb.backend.tm.TransactionManager;
import org.junit.Test;

/**
 * @author: Tjyy
 * @date: 2024-02-28 10:14
 * @description: test
 */
public class TestTransactionManager {
    public static final String path = "./transaction";
    public static final String fail_path = "./transaction_fail";

    @Test
    public void testCreateTransactionManager(){
        System.out.println("测试创建新建 TransactionManager");
        TransactionManager transactionManager = TransactionManager.create(path);

        long xid = transactionManager.begin();
        System.out.println("测试事务 " + xid + "是否为活跃状态" + transactionManager.isActive(xid));

        transactionManager.commit(xid);
        System.out.println("测试事务 " + xid + "是否为活跃状态" + transactionManager.isActive(xid));
        System.out.println("测试事务 " + xid + "是否为完成状态" + transactionManager.isCommitted(xid));


        xid = transactionManager.begin();
        transactionManager.commit(xid);

        xid = transactionManager.begin();
        transactionManager.abort(xid);

        xid = transactionManager.begin();
        System.out.println("当前事务xid为 " + xid);
        transactionManager.commit(xid);
    }


    @Test
    public void testOpenTransactionManager(){
        System.out.println("测试打开 TransactionManager");
        TransactionManager transactionManager = TransactionManager.open(path);

        long xid = transactionManager.begin();
        System.out.println("测试事务 " + xid + "是否为活跃状态" + transactionManager.isActive(xid));

        transactionManager.commit(xid);
        System.out.println("测试事务 " + xid + "是否为活跃状态" + transactionManager.isActive(xid));
        System.out.println("测试事务 " + xid + "是否为完成状态" + transactionManager.isCommitted(xid));
    }


    @Test
    public void testOpenTransactionManagerFail(){
        System.out.println("测试打开失败 TransactionManager");
        TransactionManager transactionManager = TransactionManager.open(fail_path);
    }
}
