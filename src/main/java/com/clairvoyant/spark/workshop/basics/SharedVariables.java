package com.clairvoyant.spark.workshop.basics;
/**
 * Created by vijaydatla on 30/05/17.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SharedVariables {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Turning log level to WARN..
        sc.setLogLevel("WARN");

        // counting numbers..
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(data, 3);


        // final int[] counter = {0};
        // Use broadcast variables to ensure the copy of the
            //  value (especially large ones) is sent to each executor node only once.
        Broadcast<Integer> broadcastVar = sc.broadcast(new Integer("0"));

        //  Accumulators..
        LongAccumulator accum = sc.sc().longAccumulator();


        JavaRDD<Integer> res = rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Integer>>() {
            @Override
            public Iterator<Integer> call(Integer integer, Iterator<Integer> integerIterator) throws Exception {
                Integer initialVal = broadcastVar.value();
                while(integerIterator.hasNext()) {
                    Integer val = integerIterator.next();
                    System.out.print("Inside mapPartitionWithIndex..(partitionIndex=" + integer + ")..(val=" + val + ")..(initialCounterVal="+initialVal+")..");

                  // The below lines will give compilation error and the framework does not allow updating the broadcast variables.
                   // broadcastVar += val;
                   // System.out.println("Current Counter[0] value: " + counter[0]);

                    initialVal += val;
                    accum.add(val);
                    System.out.println("Current Counter[0] value: " + initialVal);

                }

                return integerIterator;
            }
        }, true);

        res.collect();
        System.out.println("BroadCasted value: " + broadcastVar.value());

        System.out.println("Accumulator value: " + accum.value());

        // TIP : Use accumulators in these type of use cases where you want to update a variable based on whats happening in the executors..

        


    }
}
