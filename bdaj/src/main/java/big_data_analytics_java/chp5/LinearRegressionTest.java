package big_data_analytics_java.chp5;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class LinearRegressionTest {
	
	public static void main(String[] args) throws Exception {
	 SparkConf sc = new SparkConf().setMaster("local[*]");
	 SparkSession spark = SparkSession
	   .builder()
	   .config(sc)
	   .appName("LinearRegressionTest")
	   .getOrCreate();
	
	 Dataset<Row> fullData = spark.read().csv("data/kc_house_data.csv");
	 		fullData.createOrReplaceTempView("houses");
	 
	// Dataset<Row> trainingData = spark.sql("select _c3 bedrooms, _c2 price from houses");
	 	 Dataset<Row> trainingData = spark.sql("select _c3 bedrooms,_c4 bathrooms,_c5 sqft_living,_c6 sqft_lot, _c2 price from houses");

	 	trainingData.printSchema();

	 JavaRDD<Row> training = trainingData.javaRDD().map(s -> {
		 //System.out.println(s.getString(1) + " , " + s.getString(0));
		 return RowFactory.create(Double.parseDouble(s.getString(4).trim()),
				 	Vectors.dense( Double.parseDouble(s.getString(0).trim()),
				 				   Double.parseDouble(s.getString(1).trim()),
				 				  Double.parseDouble(s.getString(2).trim()),
				 				 Double.parseDouble(s.getString(3).trim()))
				 	);
	 });
	 
	 StructType schema = new StructType(new StructField[]{
			    new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
			    new StructField("features", new VectorUDT(), false, Metadata.empty())
			});
	 
	 Dataset<Row> trn = spark.createDataFrame(training, schema);
	 
	 trn.show();
	 
	// Prepare training and test data.
	 Dataset<Row>[] splits = trn.randomSplit(new double[] {0.9, 0.1}, 12345);
	 Dataset<Row> trainingMain = splits[0];
	 Dataset<Row> testMain = splits[1];
	 
	 LinearRegression lr = new LinearRegression()
	   .setMaxIter(50)
	   .setRegParam(0.3)
	   .setElasticNetParam(0.5);
	
	 // Fit the model.
	 LinearRegressionModel lrModel = lr.fit(trainingMain);
	
	//  Print the coefficients and intercept for linear regression.

	
	//  Summarize the model over the training set and print out some metrics.
	 LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
	 //System.out.println("numIterations: " + trainingSummary.totalIterations());
	 //System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
	 //trainingSummary.residuals().show();
	 System.out.println("Coefficients: "
			   + lrModel.coefficients() + " Intercept: " + lrModel.intercept());	 
	 System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
	 //System.out.println("r2: " + trainingSummary.r2());
	  
	 
	 Dataset<Row> results = lrModel.transform(testMain);
	 Dataset<Row> rows = results.select("features", "label", "prediction");
	 for (Row r: rows.collectAsList()) {
	   System.out.println("(" + r.get(0) + ", " + r.get(1) + ") " + ", prediction=" + r.get(2));
	 }
	
	 spark.stop();
	}

}
