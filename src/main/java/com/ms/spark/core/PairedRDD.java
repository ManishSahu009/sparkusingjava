package com.ms.spark.core;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class PairedRDD {
    public static void main(String[] args) {

        List<String> list=new ArrayList<>();
        list.add("INFO:01/01/2020");
        list.add("INFO:02/01/2020");
        list.add("ERROR:03/01/2020");
        list.add("INFO:05/01/2020");
        list.add("WARN:06/01/2020");
        list.add("INFO:07/01/2020");
        list.add("FATAL:09/01/2020");
        list.add("ERROR:10/01/2020");

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf=new SparkConf().setAppName("Test").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);

        //readable code in java
        JavaRDD<String> parallelize = sc.parallelize(list);
        JavaPairRDD<String, Integer> pairedRDD = parallelize.mapToPair(rawValue -> {
            String[] str = rawValue.split(":");
            String level = str[0];
            return new Tuple2<>(level, 1);
            
        });
        JavaPairRDD<String, Integer> reduceByKey = pairedRDD.reduceByKey((val1, val2) -> val1 + val2);
        reduceByKey.foreach(tuple -> System.out.println(tuple._1 +" has "+tuple._2 + " instances"));
        System.out.println("==============================");

        //optimized code of above operation
        sc.parallelize(list).mapToPair(ele -> new Tuple2<>(ele.split(":")[0],1L))
                .reduceByKey((val1, val2) -> val1+val2)
                .foreach(tuple -> System.out.println(tuple._1 +" has "+tuple._2 + " instances"));
        System.out.println("==============================");

        //group by example - not recommanded
        sc.parallelize(list).mapToPair(ele -> new Tuple2<>(ele.split(":")[0],1L))
                .groupByKey()
                .foreach(tuple -> System.out.println(tuple._1 +" has "+ Iterables.size(tuple._2) + " instances"));


        sc.close();
    }
}
