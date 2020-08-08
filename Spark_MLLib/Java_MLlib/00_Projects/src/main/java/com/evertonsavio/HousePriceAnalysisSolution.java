package com.evertonsavio;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.reverse;

public class HousePriceAnalysisSolution {
	
	public static void main(String[] args) {
		
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder()
				.appName("House Price Analysis")
				.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.master("local[*]").getOrCreate();
		
		Dataset<Row> csvData = spark.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("src/main/resources/kc_house_data.csv");

		//csvData.printSchema();
		//csvData.show();
		
		VectorAssembler vector = new VectorAssembler();
		vector.setInputCols(new String[]{"bedrooms", "bathrooms", "sqft_living"});
		vector.setOutputCol("features");
		Dataset<Row> csvDataWithFeatures = vector.transform(csvData);
		csvDataWithFeatures.show();

		Dataset<Row> modelInput = csvDataWithFeatures.select("features","price")
				.withColumnRenamed("price","label");

		//modelInput.show();

		Dataset<Row>[] trainAndTestData = modelInput.randomSplit(new double[]{0.8, 0.2});
		Dataset<Row> trainingData = trainAndTestData[0];
		Dataset<Row> testingData = trainAndTestData[1];

		LinearRegressionModel model = new LinearRegression().fit(trainingData);

		model.transform(testingData).show();

		System.out.println("The training data r2 value is" + model.summary().r2() + "RMSE is: " + model.summary().rootMeanSquaredError());

		System.out.println("The testing data r2 value is" + model.evaluate(testingData).r2() + "RMSE is: " + model.evaluate(testingData).rootMeanSquaredError());

	}
}