package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.streaming.Durations;

import javax.xml.crypto.Data;

public class ViewingFiguresStructuredVersion {
    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .appName("SparkStructedStream")
                .master("local[*]")
                .getOrCreate();

        spark.conf().set("spark.sql.shuffle.partitions", "10"); //Como precisa fazer um shuffle no Group By isso demora
        // pq spark reserva 200 particoes, entao e necessacio essa linha de codigo para otimizar a operacao de group para
        //poucos clusters como eu 1 notebook.

        Dataset<Row> dataframe = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "viewrecords")
                .load();

        dataframe.createOrReplaceTempView("view_figures");

        Dataset<Row> results = spark.sql("select window, cast(value as string) as course_name, " +
                "sum(5) as seconds_watched from view_figures group by window(timestamp, '1 minute'), course_name");

        StreamingQuery query = results
                .writeStream()
                .format("console")
                .outputMode(OutputMode.Update())
                .option("truncate", false)
                .option("numRows", 50)
                //.trigger(Trigger.ProcessingTime(Durations.seconds(5))) //Default e assim que novo dado chega. Melhor caso.
                .start();

        query.awaitTermination();

    }
}