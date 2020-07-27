package com.evertonsavio;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bouncycastle.asn1.dvcs.Data;

import static org.apache.spark.sql.functions.col;

public class SqlSpark {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .csv("src/main/resources/exams/students.csv");

        dataset.createOrReplaceTempView("my_students_view"); //Its like the name of TABLE

        Dataset<Row> frenchResult = spark.sql("select * from my_students_view where subject= 'French'");
        Dataset<Row> avgResult = spark.sql("select max(score), avg(score) from my_students_view where subject" +
                "= 'French'");
        Dataset<Row> distinctResult = spark.sql("select distinct(year) from my_students_view where subject = " +
                "'French' order by year");

        //Dataset<Row> modernArtResults = dataset.filter(col("subject").equalTo("Modern Art")
        //        .and(col("year").geq(2007)));

        frenchResult.show();
        avgResult.show();
        distinctResult.show();

        spark.close();

    }
}
