package com.tjyy.juc.example;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author: Tjyy
 * @date: 2024-02-29 18:45
 * @description:
 */
public class CountDownLatchExample {
    // 请求数量
    private static final int THREAD_COUNT = 550;

    @Test
    public void testCountDownLatch() throws InterruptedException{
        // 创建一个具有固定线程数量的线程池对象（如果这里线程池的线程数量给太少的话你会发现执行的很慢）
        // 只是测试使用，实际场景请手动赋值线程池参数
        ExecutorService threadPool = Executors.newFixedThreadPool(300);
        final CountDownLatch countDownLatch = new CountDownLatch(300);

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadNum = i;
            threadPool.execute(()->{
                try {
                    test(threadNum);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }finally {
                    // 表示一个请求已经被完成
                    countDownLatch.countDown();
                }
            });
        }

        countDownLatch.await();
        threadPool.shutdown();
        System.out.println("finish");
    }



    public static void test(int threadnum) throws InterruptedException{
        Thread.sleep(1000);// 模拟请求的耗时操作
        System.out.println("thread num:" + threadnum);
        Thread.sleep(1000);// 模拟请求的耗时操作
    }
}
