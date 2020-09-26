package dev.evertonsavio;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionSummary;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import javax.xml.crypto.Data;

public class SparkLogisticRegression {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("PredictiveMaintenance")
                .master("local[*]")
                .config("spark.driver.extraClassPath","mssql-jdbc-8.2.2.jre8.jar")
                .getOrCreate();

        System.out.println(spark);

        Dataset<Row> df1 = spark.read().format("jdbc")
                .option("url", "jdbc:sqlserver://localhost:1433;databaseName=OPCServiceBus")
                .option("dbtable", "dbo.df_2638_Norm")
                .option("user", "sa")
                .option("password", "sa")
                .option("inferSchema", true)
                .load();

        Dataset<Row> df2 = spark.read().format("jdbc")
                .option("url", "jdbc:sqlserver://localhost:1433;databaseName=OPCServiceBus")
                .option("dbtable", "dbo.df_2640_Norm")
                .option("user", "sa")
                .option("password", "sa")
                .option("inferSchema", true)
                .load();

        Dataset<Row> df = df1.union(df2);

        df.show();

        df = df.filter("Label_VIb != 0.0 AND Label_VIb != 2.0")
                .withColumnRenamed("Label_VIb", "label");

        df = df.withColumn("label", df.col("label").cast("float"));
        df = df.withColumn("label", functions
                .when(functions.col("label").$less(2),100)
                .otherwise(200));

        df = df.withColumn("label", functions
                .when(functions.col("label").$greater(150),1)
                .otherwise(0));


        //df.select("label").distinct().show();

        df = df.where(functions.col("Inicio_da_Avaria_DateTime")
                .between("2019-11-19","2020-06-01"));

        //df.createOrReplaceTempView("vibimp");
        //System.out.println(df.select("label").count());

        df = df.withColumn("Max_Imp", df.col("Max_Imp").cast("float"));
        df = df.withColumn("Std_Imp", df.col("Std_Imp").cast("float"));
        df = df.withColumn("Media_Imp", df.col("Media_Imp").cast("float"));
        df = df.withColumn("Moda_Imp", df.col("Moda_Imp").cast("float"));
        df = df.withColumn("Max_Vib", df.col("Max_Vib").cast("float"));
        df = df.withColumn("Std_Vib", df.col("Std_Vib").cast("float"));
        df = df.withColumn("Media_VIb", df.col("Media_VIb").cast("float"));
        df = df.withColumn("Moda_Vib", df.col("Moda_Vib").cast("float"));

        VectorAssembler vector = new VectorAssembler()
                .setInputCols(new String[]{"Max_Imp", "Std_Imp","Media_Imp", "Max_Vib", "Std_Vib", "Media_VIb"})
                .setOutputCol("features");

        df = vector.transform(df);

        Dataset<Row>[] datasplits = df.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainingAndTestData = datasplits[0];
        Dataset<Row> holdOutData = datasplits[1];

        org.apache.spark.ml.classification.LogisticRegression logisticRegression = new org.apache.spark.ml.classification.LogisticRegression();
        ParamGridBuilder paramGridBuilder = new ParamGridBuilder();

        ParamMap[] paramMaps = paramGridBuilder.addGrid(logisticRegression.regParam(), new double[]{0.01, 0.25, 0.5, 0.75,1})
                .addGrid(logisticRegression.elasticNetParam(), new double[]{0, 0.25, 0.5, 0.75, 1})
                .build();

        TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
                .setEstimator(logisticRegression)
                .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
                .setEstimatorParamMaps(paramMaps)
                .setTrainRatio(0.8);

        TrainValidationSplitModel model = trainValidationSplit.fit(trainingAndTestData);
        LogisticRegressionModel lrModel = (LogisticRegressionModel) model.bestModel();

        System.out.println("The R2 value is " + lrModel.summary().accuracy());

        System.out.println("coefficients : " + lrModel.coefficients() + " intercept : " + lrModel.intercept());
        System.out.println("reg param : " + lrModel.getRegParam() + " elastic net param : " + lrModel.getElasticNetParam());

        LogisticRegressionSummary summary = lrModel.evaluate(holdOutData);

        double truepositives = summary.truePositiveRateByLabel()[1];
        double falsepositives = summary.falsePositiveRateByLabel()[0];

        System.out.println("For houtoutdata, the likehood of positive being correct is " + truepositives / (truepositives + falsepositives));
        System.out.println("For houtoutdata, data accuracy " + summary.accuracy());

        lrModel.transform(holdOutData).groupBy("label", "prediction").count().show();

    }

}
