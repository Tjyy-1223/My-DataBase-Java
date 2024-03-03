package com.tjyy.juc.example;

import org.junit.Test;

import java.util.concurrent.*;

/**
 * @author: Tjyy
 * @date: 2024-02-29 18:15
 * @description: semaphore
 */
public class SemaphoreExample {
    // 请求数量
    private static final int threadCount = 550;

    @Test
    public void testSemaphoreFunc() throws InterruptedException{
        // 创建一个具有固定线程数量的线程池对象（如果这里线程池的线程数量给太少的话你会发现执行的很慢）
        ExecutorService threadPool = Executors.newFixedThreadPool(300);

        // 初始许可证数量
        final Semaphore semaphore = new Semaphore(20);

        for (int i = 0; i < threadCount; i++) {
            final int threadnum = i;
            threadPool.execute(() -> {
                try{
                    semaphore.acquire(); // 获取一个许可，所以可运行线程数量为20/1=20
                    SemaphoreExample.test(threadnum);
                    semaphore.release(); // 释放一个许可
                }catch (InterruptedException e){
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            });
        }

        Thread.sleep(10000);
        threadPool.shutdown();
        System.out.println("finish");
    }

    public static void test(int threadnum) throws InterruptedException{
        Thread.sleep(1000);// 模拟请求的耗时操作
        System.out.println("thread num:" + threadnum);
        Thread.sleep(1000);// 模拟请求的耗时操作
    }
}
