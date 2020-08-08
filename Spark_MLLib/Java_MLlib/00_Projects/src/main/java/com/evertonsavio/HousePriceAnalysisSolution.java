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

		///////////////////////Criando vetor de Features///////////////////////////////////////////////////////////

		csvData = csvData.withColumn("sqft_above_percentage", col("sqft_above").divide(col("sqft_living")));

		VectorAssembler vector = new VectorAssembler();
		vector.setInputCols(new String[]{"bedrooms", "bathrooms", "sqft_living", "sqft_above_percentage", "floors"});
		vector.setOutputCol("features");
		Dataset<Row> csvDataWithFeatures = vector.transform(csvData);
		csvDataWithFeatures.show();

		Dataset<Row> modelInput = csvDataWithFeatures.select("features","price")
				.withColumnRenamed("price","label");

		//modelInput.show();

		//////////////////////Spliting data em test treino e Hold Out Data devido ao GRID///////////////////////////

		Dataset<Row>[] dataSplits = modelInput.randomSplit(new double[]{0.8, 0.2});
		Dataset<Row> trainingAndTestData = dataSplits[0];
		Dataset<Row> holdOutData = dataSplits[1];

		//////////////////////Construir e Lidar com os parametros do modelo atraves de GRID////////////////////////

		LinearRegression linearRegression = new LinearRegression();
		ParamGridBuilder paramGridBuilder = new ParamGridBuilder();

		ParamMap[] paramMaps = paramGridBuilder
				.addGrid(linearRegression.regParam(), new double[]{0.01, 0.1, 0.5})
				.addGrid(linearRegression.elasticNetParam(), new double[] {0, 0.5,1})
				.build();

		TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
				.setEstimator(linearRegression)
				.setEvaluator(new RegressionEvaluator().setMetricName("r2"))
				.setEstimatorParamMaps(paramMaps)
				.setTrainRatio(0.8);

		TrainValidationSplitModel model = trainValidationSplit.fit(trainingAndTestData);
		LinearRegressionModel lrmodel = (LinearRegressionModel) model.bestModel();

		//LinearRegressionModel model = new LinearRegression()
		//		.setMaxIter(10) // Olhar documentacao;
		//		.setRegParam(0.3)
		//		.setElasticNetParam(0.8)
		//		.fit(trainingData);
		//model.transform(testingData).show();
		//System.out.println("The training data r2 value is" + model.summary().r2() + "RMSE is: " + model.summary().rootMeanSquaredError());
		//System.out.println("The testing data r2 value is" + model.evaluate(testingData).r2() + "RMSE is: " + model.evaluate(testingData).rootMeanSquaredError());

		System.out.println("The training data r2 value is" + lrmodel.summary().r2() + "RMSE is: " + lrmodel.summary().rootMeanSquaredError());

		System.out.println("The testing data r2 value is" + lrmodel.evaluate(holdOutData).r2() + "RMSE is: " + lrmodel.evaluate(holdOutData).rootMeanSquaredError());

		System.out.println("Coeficients: "+ lrmodel.coefficients() + "Intecept: "+ lrmodel.intercept());

		System.out.println(lrmodel.getRegParam() + " " +lrmodel.getElasticNetParam());
	}
}