package com.ms.spark.core;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Locale;

public class KeyWordRanking {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf=new SparkConf().setAppName("Test").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> fileRdd = sc.textFile("src/main/resources/subtitles/input.txt");
        JavaRDD<String> worldRdd = fileRdd.map(sentnc -> sentnc.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
        JavaRDD<String> justWord = worldRdd.filter(str -> str.trim().length() > 0);
        JavaRDD<String> interWord = justWord.flatMap(str -> Arrays.asList(str.split(" ")).iterator());
        JavaRDD<String> jsutInterestingWord = interWord.filter(word -> Util.isNotBoring(word));
        JavaPairRDD<String, Long> pairRDD = jsutInterestingWord.mapToPair(word -> new Tuple2<>(word, 1L));
        JavaPairRDD<String, Long> reducedRDD = pairRDD.reduceByKey((val1, val2) -> val1 + val2);
        JavaPairRDD switched = reducedRDD.mapToPair(tuple -> new Tuple2(tuple._2, tuple._1));
        JavaPairRDD<String, Long> sortRDD = switched.sortByKey(false);

        //WARNING :
        // ll give wrong result
        // default partition in spark is 2 if running in local
        // as 2 thread will run parallelly and try to print on console even though data is sorted within partition
        // but result will not be sorted
        //sortRDD.foreach(System.out::println);

        // to check no. of partitions
        //System.out.println(sortRDD.getNumPartitions());


        sortRDD.collect().forEach(System.out::println);


    }
}
