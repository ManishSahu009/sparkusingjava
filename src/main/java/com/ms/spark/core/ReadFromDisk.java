package com.ms.spark.core;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReadFromDisk {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir","c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf=new SparkConf().setAppName("Test").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> fileRdd = sc.textFile("src/main/resources/subtitles/input.txt");
        fileRdd.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .collect().forEach(System.out::println);

        sc.close();
    }
}
