package com.clairvoyant.spark.workshop.basics;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by vijaydatla on 01/06/17.
 */
public class StructuredStreamingWindow {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder().master("local[2]")
                .appName("JavaStructuredNetworkWordCount")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .option("includeTimestamp", true)
                .load();

        // Split the lines into words
        Dataset<Row> words = lines
                .as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
                .flatMap(
                        new FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>() {
                            @Override
                            public Iterator<Tuple2<String, Timestamp>> call(Tuple2<String, Timestamp> t) {
                                List<Tuple2<String, Timestamp>> result = new ArrayList<>();
                                for (String word : t._1.split(" ")) {
                                    result.add(new Tuple2<>(word, t._2));
                                }
                                return result.iterator();
                            }
                        },Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()) ).toDF("word", "timestamp");

        // Generate running word count
        Dataset<Row> wordCounts = words.groupBy(functions.window(words.col("timestamp"), "10 seconds"),words.col("word")).count();

        // Start running the query that prints the running counts to the console
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("update")
                .format("console")
                .start();

        try {
            query.awaitTermination();
        }catch (Exception e)
        {

        }

    }

}
