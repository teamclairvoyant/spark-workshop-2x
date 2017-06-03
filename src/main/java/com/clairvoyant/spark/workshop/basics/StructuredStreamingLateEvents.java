package com.clairvoyant.spark.workshop.basics;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.ProcessingTime;
import org.apache.spark.sql.streaming.StreamingQuery;
import scala.Tuple2;
import scala.Tuple3;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by vijaydatla on 01/06/17.
 */
public class StructuredStreamingLateEvents {

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
                        new FlatMapFunction<Tuple2<String, Timestamp>, Tuple3<String, Timestamp, Timestamp>>() {
                            @Override
                            public Iterator<Tuple3<String, Timestamp, Timestamp>> call(Tuple2<String, Timestamp> t) {
                                List<Tuple3<String, Timestamp, Timestamp>> result = new ArrayList<>();
                                String[] event = t._1().split("    ");
                                try {
                                    result.add(new Tuple3<>(event[1], Timestamp.valueOf(event[0]), t._2()));
                                }catch (Exception e)
                                {
                                    result.add(new Tuple3<>("Error", Timestamp.valueOf("2000-01-01 00:00:00.000000"), t._2()));
                                }
                                return result.iterator();
                            }
                        },Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP(), Encoders.TIMESTAMP()) ).toDF("word", "eventtime", "timestamp");

        // Generate running word count
        Dataset<Row> wordCounts = words
                .groupBy(functions.window(words.col("eventtime"), "30 seconds", "15 seconds"),words.col("word")).count();



        // Start running the query that prints the running counts to the console
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("update")
                .trigger(ProcessingTime.create(2, TimeUnit.SECONDS))
                .format("console")
                .option("truncate", "false")
                .start();

        try {
            query.awaitTermination();
        }catch (Exception e)
        {

        }

    }

}
