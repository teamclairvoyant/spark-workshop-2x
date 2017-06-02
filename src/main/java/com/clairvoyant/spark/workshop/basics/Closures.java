package com.clairvoyant.spark.workshop.basics; /**
 * Created by vijaydatla on 30/05/17.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Closures {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Turning log level to WARN..
        sc.setLogLevel("WARN");

        // counting numbers..
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(data, 3);

        final int[] counter = {0};

        JavaRDD<Integer> res = rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Integer>>() {
            @Override
            public Iterator<Integer> call(Integer integer, Iterator<Integer> integerIterator) throws Exception {
                Integer initialVal = counter[0];
                while(integerIterator.hasNext()) {
                    Integer val = integerIterator.next();
                    System.out.print("Inside mapPartitionWithIndex..(partitionIndex=" + integer + ")..(val=" + val + ")..(initialCounterVal="+initialVal+")..");

                    counter[0] += val;
                    System.out.println("Current Counter[0] value: " + counter[0]);
                }

                return integerIterator;
            }
        }, true);

        res.collect();
        System.out.println("Counter value: " + counter[0]);


        // TIP : Use accumulators in these type of use cases where you want to update a variable
        // based on whats happening in the executors..




    }
}
