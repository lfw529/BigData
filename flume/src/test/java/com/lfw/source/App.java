package com.lfw.source;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class App {
    public static void main(String[] args) throws IOException {
        File positionFile = new File("E:\\IdeaProject\\bigdata\\flume\\src\\main\\resources\\myposition.txt");
        String s = FileUtils.readFileToString(positionFile);
        long offset = Long.parseLong(s);
        System.out.println(offset);
    }
}
