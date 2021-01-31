import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<Double> list=new ArrayList<>();
        list.add(2.2);
        list.add(2.5);
        list.add(2.8);
        list.add(2.11);
        list.add(9.8);
        list.add(99.0);
        list.add(2.99);
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf=new SparkConf().setAppName("Test").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);

        //reduce example
        JavaRDD<Double> parallelize = sc.parallelize(list);
        Double reduce = parallelize.reduce((value1, value2) -> value1 + value2);
        System.out.println(reduce);

        //map example
        JavaRDD<Double> sqrtmap = parallelize.map(element -> Math.sqrt(element));
        sqrtmap.foreach(element -> System.out.println(element));

        //custom count example
        JavaRDD<Long> countRdd = sqrtmap.map(ele -> 1L);
        Long count=countRdd.reduce((e1, e2)-> e1+e2);
        System.out.println(count);


        sc.close();
    }
}
