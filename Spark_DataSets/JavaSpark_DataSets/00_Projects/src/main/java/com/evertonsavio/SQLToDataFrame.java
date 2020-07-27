package com.evertonsavio;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
// import org.apache.spark.sql.functions; // -> functions.date_format
import static org.apache.spark.sql.functions.*; // -> date_format

public class SQLToDataFrame {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

        //dataset = dataset.selectExpr("level", "date_format(datetime,'MMMM') as month");
        
        dataset = dataset.select(col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("monthnum"));

        dataset = dataset.groupBy(col("level"), col("month"))
                .count()
                .orderBy(first("monthnum").cast(DataTypes.IntegerType), first("level"));;


        dataset.show();

        spark.close();

    }
}
