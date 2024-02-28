package com.tjyy.mydb.util;

import com.tjyy.mydb.backend.util.ParseStringRes;
import com.tjyy.mydb.backend.util.Parser;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author: Tjyy
 * @date: 2024-02-25 21:04
 * @description: parser test
 */
public class ParserTest {
    @Test
    public void testString2Bytes(){
        String str = "tjyy";
        System.out.println(str.length());
        System.out.println(str.getBytes());

        byte[] array = Parser.string2Byte(str);
        System.out.println(array);
        System.out.println(Arrays.copyOfRange(array, 4, 4 + 4));

        ParseStringRes res = Parser.parseString(array);
        System.out.println(res);
    }
}
