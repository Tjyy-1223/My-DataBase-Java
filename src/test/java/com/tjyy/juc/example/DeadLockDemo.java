package com.tjyy.juc.example;

import org.junit.Test;

/**
 * @author: Tjyy
 * @date: 2024-03-02 16:23
 * @description: dead lock
 */
public class DeadLockDemo {
    private static Object resource1 = new Object();
    private static Object resource2 = new Object();

    @Test
    public void TestDeadLock() throws InterruptedException {
        new Thread(() ->{
           synchronized (resource1){
               System.out.println(Thread.currentThread() + "get resource1");
               try {
                   Thread.sleep(1000);
               }catch (InterruptedException e){
                   e.printStackTrace();
               }
               System.out.println(Thread.currentThread() + "waiting get resource2");
               synchronized (resource2){
                   System.out.println(Thread.currentThread() + "get resource2");
               }
           }
        }, "thread - 1").start();

        new Thread(() ->{
            synchronized (resource2){
                System.out.println(Thread.currentThread() + "get resource2");
                try {
                    Thread.sleep(1000);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
                System.out.println(Thread.currentThread() + "waiting get resource1");
                synchronized (resource1){
                    System.out.println(Thread.currentThread() + "get resource1");
                }
            }
        }, "thread - 2").start();

        Thread.sleep(5000);
    }
}
