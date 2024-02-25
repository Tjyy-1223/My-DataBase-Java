package com.tjyy.mydb.util;

/**
 * @author: Tjyy
 * @date: 2024-02-25 20:36
 * @description: panic
 */
public class Panic {
    public static void panic(Exception e){
        e.printStackTrace();
        System.exit(1);
    }
}
