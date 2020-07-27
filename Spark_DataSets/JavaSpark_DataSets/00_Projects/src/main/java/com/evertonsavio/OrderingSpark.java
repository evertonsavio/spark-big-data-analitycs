package com.evertonsavio;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bouncycastle.asn1.dvcs.Data;

public class OrderingSpark {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

        dataset.createOrReplaceTempView("logging_table");

//        Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month, " +
//                "cast(first(date_format(datetime, 'M')) as int) as monthnum, count(1) as total " +
//                "from logging_table group by level, month order by monthnum");
        Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month " +
                ", count(1) as total from logging_table group by level, month " +
                "order by cast(first(date_format(datetime, 'M')) as int), level");

//        results = results.drop("monthnum");

        results.show(100);

        spark.close();

    }
}
