package com.evertonsavio;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;


public class PracticalSpark {

    @SuppressWarnings("resource")
    public static void main(String[] args)
    {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> dataset = spark.read()
                .option("header", true)
                //.option("inferSchema", true) //Performance Issues, prefer CAST
                .csv("src/main/resources/exams/students.csv");

        //Column score = dataset.col("score");
        // functions.max -> max

        //dataset = dataset.groupBy("subject").agg(max("score").cast(DataTypes.IntegerType)
        //        .alias("maxcol") , min("score")
        //        .alias("minscore")); // Need Cast or not? NO! because o max of agg

        //dataset.select("year").distinct().show();

        dataset.groupBy("subject").pivot("year").agg(round (avg(col("score")), 2).alias("average"),
                round(stddev(col("score")),2).alias("std")).show();

        //dataset.show();
        spark.close();

    }

}