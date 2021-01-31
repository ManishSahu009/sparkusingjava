package com.ms.spark.core;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Tuples {
    public static void main(String[] args) {

        List<Integer> list=new ArrayList<>();
        list.add(3);
        list.add(4);
        list.add(8);
        list.add(9);

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf=new SparkConf().setAppName("Test").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        //Tuple2<Integer,Double> myvalue=new Tuple2<>(9,3.0); - here 2 indicates the no. of element in tuple
        JavaRDD<Integer> parallelize = sc.parallelize(list);
        JavaRDD<Tuple2<Integer, Double>> sqrtRDD=parallelize.map(ele -> new Tuple2<>(ele, Math.sqrt(ele)));
        sqrtRDD.take(2).forEach(System.out::println);

        sc.close();
    }
}
