package com.evertonsavio;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import javax.xml.crypto.Data;
import java.util.Arrays;
import java.util.List;

public class VPPFreeTrialsSolution {

	public static UDF1<String,String> countryGrouping = new UDF1<String,String>() {

		@Override
		public String call(String country) throws Exception {
			List<String> topCountries =  Arrays.asList(new String[] {"GB","US","IN","UNKNOWN"});
			List<String> europeanCountries =  Arrays.asList(new String[] {"BE","BG","CZ","DK","DE","EE","IE","EL","ES","FR","HR","IT","CY","LV","LT","LU","HU","MT","NL","AT","PL","PT","RO","SI","SK","FI","SE","CH","IS","NO","LI","EU"});

			if (topCountries.contains(country)) return country;
			if (europeanCountries .contains(country)) return "EUROPE";
			else return "OTHER";
		}

	};

	public static void main(String[] args) {

		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder()
				.appName("VPP Free Trieals")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.master("local[*]").getOrCreate();

		spark.udf().register("countryGrouping", countryGrouping, DataTypes.StringType);

		Dataset<Row> dataset = spark.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("src/main/resources/VPPFreeTrials.csv");

		dataset.show();

		dataset = dataset.withColumn("country", functions.callUDF("countryGrouping", functions.col("country")))
				.withColumn("label", functions.when(functions.col("payments_made")
						.geq(1), functions.lit(1)).otherwise(functions.lit(0)));

		StringIndexer countryIndexer = new StringIndexer();

		dataset = countryIndexer.setInputCol("country")
				.setOutputCol("countryIndex")
				.fit(dataset).transform(dataset);


		new IndexToString()
				.setInputCol("countryIndex")
				.setOutputCol("value")
				.transform(dataset.select("countryIndex").distinct())
				.show();

		VectorAssembler vectorAssembler = new VectorAssembler();
		vectorAssembler.setInputCols(new String[]{"countryIndex", "rebill_period", "chapter_access_count", "seconds_watched"})
				.setOutputCol("features");

		Dataset<Row> inputData = vectorAssembler.transform(dataset).select("label", "features");
		inputData.show();

		Dataset<Row>[] trainingAndHoldData = inputData.randomSplit(new double[] {0.8, 0.2});
		Dataset<Row> trainingData = trainingAndHoldData[0];
		Dataset<Row> houdoutData = trainingAndHoldData[1];

		DecisionTreeClassifier dtc = new DecisionTreeClassifier();

		dtc.setMaxDepth(5);

		DecisionTreeClassificationModel model = dtc.fit(trainingData);

		Dataset<Row> predictions = model.transform(houdoutData);
		predictions.show();

		System.out.println(model.toDebugString());

		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();
		evaluator.setMetricName("accuracy");
		System.out.println("Accuracy: " + evaluator.evaluate(predictions));

		////////////////////////RANDOM FOREST/////////////////////
		RandomForestClassifier randomForestClassifier = new RandomForestClassifier();
		randomForestClassifier.setMaxDepth(5);
		RandomForestClassificationModel rfmodel = randomForestClassifier.fit(trainingData);
		Dataset<Row> predictions2 = rfmodel.transform(houdoutData);

		predictions2.show();
		System.out.println(rfmodel.toDebugString());
		System.out.println("Accuracy Random Forest: " + evaluator.evaluate(predictions2));




	}

}
