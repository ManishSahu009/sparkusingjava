package com.ms.spark.core;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlatMaps {
    public static void main(String[] args) {

        List<String> list=new ArrayList<>();
        list.add("INFO: 01/01/2020");
        list.add("INFO: 02/01/2020");
        list.add("ERROR: 03/01/2020");
        list.add("INFO: 05/01/2020");
        list.add("WARN: 06/01/2020");
        list.add("INFO: 07/01/2020");
        list.add("FATAL: 09/01/2020");
        list.add("ERROR: 10/01/2020");

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf=new SparkConf().setAppName("Test").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> parallelize = sc.parallelize(list);
        JavaRDD<String> flatMapRDD = parallelize.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
        flatMapRDD.collect().forEach(System.out::println);

        sc.close();
    }
}
