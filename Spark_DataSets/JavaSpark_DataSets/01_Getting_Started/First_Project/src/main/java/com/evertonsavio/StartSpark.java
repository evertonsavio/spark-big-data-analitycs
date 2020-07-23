package com.evertonsavio;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import java.util.logging.Level;
import java.util.logging.Logger;

public class StartSpark {

    @SuppressWarnings("resource")
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARNING);

        SparkSession spark = SparkSession.builder().appName("FirstProject")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/") // FOR WINDOWS USERS
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
        dataset.show();
        //long numberOfRows = dataset.count();
        //System.out.println(numberOfRows);
        //Dataset<Row> mathResult = dataset.filter(col("subject").equalTo("Math").and)

        spark.close();
    }

}
