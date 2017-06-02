package com.clairvoyant.spark.workshop.basics; /**
 * Created by vijaydatla on 30/05/17.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;

public class Functions {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Turning log level to WARN..
        sc.setLogLevel("WARN");

        // WordsFromFile
        JavaRDD<String> distFile = sc.textFile("data.txt");

        // Caching the rdd..
        //distFile.cache();
        distFile.persist(StorageLevel.MEMORY_ONLY());

        // Passing Functions to Spark

        class GetLength implements Function<String, Integer> {
            public Integer call(String s) { return s.length(); }
        }
        JavaRDD<Integer> lineLengths = distFile.map(new GetLength());

        // Anonymous Function..
        int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) { return a + b; }
        }
        );

        System.out.println("Functions example output:");
        System.out.println(totalLength);

    }
}
