package com.tjyy.mydb.util;

import lombok.Data;

/**
 * @author: Tjyy
 * @date: 2024-02-25 20:51
 * @description: parse string
 */
@Data
public class ParseStringRes {
    public String str;
    public int next;

    public ParseStringRes(String str, int next) {
        this.str = str;
        this.next = next;
    }
}
