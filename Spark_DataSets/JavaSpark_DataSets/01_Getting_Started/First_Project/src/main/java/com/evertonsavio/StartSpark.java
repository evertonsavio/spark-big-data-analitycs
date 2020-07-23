package com.evertonsavio;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class StartSpark {

    @SuppressWarnings("resource")
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("FirstProject")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/") // FOR WINDOWS USERS
                .getOrCreate();

        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .csv("src/main/resources/exams/students.csv");

        //dataset.show();
        //long numberOfRows = dataset.count();
        //System.out.println(numberOfRows);
        /////////////////////////////////////////////////////////////////////////
        ///FILTER

        Dataset<Row> mathResult = dataset.filter(col("subject")
                .equalTo("Math")
                .and(col("year").equalTo("2005")));

        Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year == 2007");

        /////////////////////////////////////////////////////////////////////////
        ///SHOW

        modernArtResults.show();
        mathResult.show();
        /////////////////////////////////////////////////////////////////////////

        Row firstRow = dataset.first();
        //String subject = firstRow.get(2).toString();
        String subject = firstRow.getAs("subject").toString();
        System.out.println(subject);

        int year = Integer.parseInt(firstRow.getAs("year").toString());
        System.out.println("Year is :" + year);

        spark.close();
    }

}
