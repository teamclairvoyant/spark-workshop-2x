package com.clairvoyant.spark.workshop.basics; /**
 * Created by vijaydatla on 30/05/17.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class WordCount {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Turning log level to WARN..
        sc.setLogLevel("WARN");

        // counting numbers..
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        System.out.println(distData.count());

        // WordsFromFile  (with partitions)
        JavaRDD<String> distFile = sc.textFile("data.txt",3);

        // Caching the rdd..
        //distFile.cache();
        distFile.persist(StorageLevel.MEMORY_ONLY());

        // WordsFromFile
        System.out.println("### Words from file ###");
        System.out.println(distFile.collect());

        // total wordcount
        System.out.println("### Total no.of Words (.count()) ###");
        long totalWordsCount = distFile.count();
        System.out.println(totalWordsCount);

        // total distinctWordCount
        System.out.println("### Total Distinct Word Count (.count())###");
        long totalDistinctWordsCount = distFile.distinct().count();
        System.out.println(totalDistinctWordsCount);

        // total wordcount ()
        System.out.println("### Total no.of Words (not effective) ###");
        Integer totalWords = distFile.map(w -> new Integer("1")).reduce((x, y) -> (x + y));
        System.out.println(totalWords);

        // counts by word
        System.out.println("### Word Count ###");
        JavaPairRDD<String, Integer> totalDistinctWords = distFile.mapToPair(w ->
                new Tuple2<String, Integer>(w, new Integer("1"))).reduceByKey((x,y) -> x+y);
        System.out.println(totalDistinctWords.collectAsMap());

        //Waiting for some time to explore the SparkUI
        try {
            TimeUnit.MINUTES.sleep(5);
          } catch (InterruptedException e) {
            //Handle exception
        }


    }
}
