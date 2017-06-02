package com.clairvoyant.spark.workshop.basics; /**
 * Created by vijaydatla on 30/05/17.
 */

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SystemClock;

import static org.apache.spark.sql.functions.col;

import java.io.Serializable;
import java.sql.Date;
import java.util.concurrent.TimeUnit;

public class RDDToDataSets {

    public static class Person implements Serializable {
        private String name;
        private int age;

        public Date getRegDate() {
            return regDate;
        }

        public void setRegDate(Date regDate) {
            this.regDate = regDate;
        }

        private Date regDate;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

    public static void main(String[] args) {

        // Single point of entry to spark..

        System.out.println("#####    Creating Spark session ...");
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // Creating JavaRDD from a json file..

        System.out.println("#####    Creating JavaRDD from a json file..");
        JavaRDD<String> personRDD = spark.sparkContext().textFile("src/main/resources/people.json", 1).toJavaRDD();

        System.out.println("***    Here is the data..");

        personRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });


        // Converting JavaRDD to DataFrame (DataSet<Row>) ..   Types AutoInferred based on data..

        System.out.println("#####    Converting JavaRDD to DataFrame (DataSet<Row>)..  (Types AutoInferred based on data)");

        Dataset<Row> personDF_TypesFromData = spark.read().json(personRDD);
        System.out.println("Data: ");
        personDF_TypesFromData.show();
        System.out.println("Schema of DataFrame: (Inferred From Data..)");
        personDF_TypesFromData.printSchema();

        //  Creating the encoder for Person..
        Encoder<Person> personEncoder = Encoders.bean(Person.class);

        System.out.println("#####    Converting DataFrame (Dataset<Row>) to Dataset (Dataset<Person)>)...");

        Dataset<Person> personDS_TypesFromData = personDF_TypesFromData.as(personEncoder);
        System.out.println("#####    Schema of Dataset created from DataFrame (types inferred from data) by passed PersonEncoder..  : ");
        System.out.println("          Note that the types of the fields still are the inferred once and not the once present in the encoder..  : ");

        personDS_TypesFromData.printSchema();

        // Converting JavaRDD to DataFrame (DataSet<Row>) ..   Types AutoInferred based on Provided Schema..

        StructType schema = DataTypes
                .createStructType(new StructField[] {
                        DataTypes.createStructField("age", DataTypes.IntegerType, true),
                        DataTypes.createStructField("name", DataTypes.StringType, true),
                        DataTypes.createStructField("regDate", DataTypes.DateType, true)
                        });

        System.out.println("#####    Converting JavaRDD to DataFrame (DataSet<Row>)..  (Types Inferred based on SchemaType)");

        Dataset<Row> personDF_TypesFromSchemaType = spark.read().schema(schema).json(personRDD);
        System.out.println("Schema of DataFrame: (Inferred From Schema..)");
        personDF_TypesFromSchemaType.printSchema();

        System.out.println("#####    Converting DataFrame (Dataset<Row>) to Dataset (Dataset<Person)>)...");

        Dataset<Person> personDS_TypesFromSchemaType = personDF_TypesFromSchemaType.as(personEncoder);
        System.out.println("#####    Schema of Dataset created from DataFrame (types inferred from SchemaType) by passed PersonEncoder..  : ");
        System.out.println("         Note that the types of the fields are the inferred once..  : ");

        personDS_TypesFromSchemaType.printSchema();


        System.out.println("#####    Filtering the dataframe...  (  col(\"age\").gt(21)  )");
        //  Code to demonstrate the compilation errors when DataFrames are used..
         personDF_TypesFromData.filter(col("regDate").gt(21)).show();
        personDF_TypesFromData.filter(col("age").gt(21)).show();
//        personDF_TypesFromData.filter((FilterFunction<Row>) row -> (row.getInt(0) < 21)).show();

        System.out.println("#####    Filtering the dataframe...  (  person.getAge() < new Integer(\"21\"))  )");
        //peopleDataSet.filter((FilterFunction<Person>) person -> (person.getRegDate() < 21));

        personDS_TypesFromSchemaType.filter((FilterFunction<Person>) person -> (person.getAge() > 21)).show();





        //Waiting for some time to explore the SparkUI
     /*   try {
            TimeUnit.MINUTES.sleep(5);
          } catch (InterruptedException e) {
            //Handle exception
        }*/


    }
}

