package com.tjyy.juc.example;

import org.junit.Test;

import java.util.Comparator;
import java.util.concurrent.*;

/**
 * @author: Tjyy
 * @date: 2024-02-29 19:02
 * @description:
 */
public class CyclicBarrierExample2 {
    // 请求的数量
    private static final int threadCount = 550;

    // 需要同步的线程数量
    private static final CyclicBarrier cyclicBarrier = new CyclicBarrier(5, ()->{
        System.out.println("------当线程数达到之后，优先执行------");
    });

    @Test
    public void testCyclicBarrier() throws InterruptedException{
        ExecutorService threadPool = Executors.newFixedThreadPool(10);
        for (int i = 0; i < threadCount; i++) {
            final int threadNum = i;
            Thread.sleep(1000);

            threadPool.execute(()->{
                try {
                    test(threadNum);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }catch (BrokenBarrierException e){
                    e.printStackTrace();
                }
            });
        }

        threadPool.shutdown();
    }


    public static void test(int threadnum) throws InterruptedException, BrokenBarrierException{
        System.out.println("threadnum:" + threadnum + "is ready");

        try {
            cyclicBarrier.await(60, TimeUnit.SECONDS);
        }catch (Exception e){
            System.out.println("-----CyclicBarrierException------");
        }

        System.out.println("threadnum:" + threadnum + "is finish");
    }
}
