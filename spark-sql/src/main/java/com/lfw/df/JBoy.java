package com.lfw.df;

/**
 * Java class 必须添加 getter 方法才能获取到 schema 信息
 */
public class JBoy {

    private Integer id;

    private String name;

    private int age;

    private double fv;

    public JBoy() {
    }

    public JBoy(Integer id, String name, int age, double fv) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.fv = fv;
    }

    public Integer getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public double getFv() {
        return fv;
    }
}
