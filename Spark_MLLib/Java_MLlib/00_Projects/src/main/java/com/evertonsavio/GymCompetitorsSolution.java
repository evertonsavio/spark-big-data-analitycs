package com.evertonsavio;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GymCompetitorsSolution {

	public static void main(String[] args) {
		
		System.setProperty("hadoop.home.dir", "c:/hadoop"); //Hadoop Home directory for Windows +_+
		Logger.getLogger("org.apache").setLevel(Level.WARN); // Switch to much Log OFF

		SparkSession spark = SparkSession.builder()
				.appName("Gym Competitors")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/") // FOR WINDOWS +_+
				.master("local[*]")
				.getOrCreate();
		
		Dataset<Row> csvData = spark.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("src/main/resources/GymCompetition.csv");

		csvData.printSchema();

		/////////////////////////////////ENCODING/////////////////////////////
		StringIndexer genderIndexer = new StringIndexer();
		genderIndexer.setInputCol("Gender");
		genderIndexer.setOutputCol("GenderIndex");
		csvData = genderIndexer.fit(csvData).transform(csvData);

		OneHotEncoderEstimator oneHotEncoderEstimator = new OneHotEncoderEstimator();
		oneHotEncoderEstimator.setInputCols(new String[]{"GenderIndex"});
		oneHotEncoderEstimator.setOutputCols(new String[]{"GenderVector"});
		csvData = oneHotEncoderEstimator.fit(csvData).transform(csvData);

		csvData.show();

		VectorAssembler vector = new VectorAssembler();
		vector.setInputCols(new String[]{"Age", "Height", "Weight", "GenderVector"});
		vector.setOutputCol("features");
		Dataset<Row> csvDataWithFeatures = vector.transform(csvData);

		Dataset<Row> modelInputData = csvDataWithFeatures.select("NoOfReps", "features").withColumnRenamed("NoOfReps", "label");

		modelInputData.show();

		LinearRegression linearRegression = new LinearRegression();

		LinearRegressionModel model = linearRegression.fit(modelInputData);
		System.out.println("Intercept: " + model.intercept() + " and coeficients: " + model.coefficients());

		model.transform(modelInputData).show(); // Using the same data to vizualize predictions. Do not do that.
	}
}