package com.clairvoyant.spark.workshop.basics;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import scala.Function1;
import static org.apache.spark.sql.functions.col;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by vijaydatla on 01/06/17.
 */
public class StructuredStreaming {

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
                .load();

        // Split the lines into words
        Dataset<String> words = lines
                .as(Encoders.STRING())
                .flatMap(
                        new FlatMapFunction<String, String>() {
                            @Override
                            public Iterator<String> call(String x) {
                                return Arrays.asList(x.split(" ")).iterator();
                            }
                        }, Encoders.STRING());

        // Generate running word count
        Dataset<Row> wordCounts = words.groupBy("value").count();

        // Start running the query that prints the running counts to the console
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        try {
            query.awaitTermination();
        }catch (Exception e)
        {

        }

    }

}
