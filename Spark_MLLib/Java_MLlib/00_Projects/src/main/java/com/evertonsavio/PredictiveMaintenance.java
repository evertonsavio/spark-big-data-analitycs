package com.evertonsavio;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

public class PredictiveMaintenance {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("PredictiveMaintenance")
                .config("spark.driver.extraClassPath","mssql-jdbc-8.2.2.jre8.jar")
                .master("local[*]")
                .getOrCreate();

        System.out.println(spark);

        Dataset<Row> dataset = spark.read().format("jdbc")
                .option("url", "jdbc:sqlserver://localhost:1433;databaseName=OPCServiceBus")
                //.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .option("dbtable", "dbo.Server")
                .option("user", "sa")
                .option("password", "sa").load();

        dataset.show();
    }

}

