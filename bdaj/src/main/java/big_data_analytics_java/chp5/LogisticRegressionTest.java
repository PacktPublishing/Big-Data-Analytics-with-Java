package big_data_analytics_java.chp5;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionTrainingSummary;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.param.ParamMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class LogisticRegressionTest {
	
	public static void main(String[] args) throws Exception {
	 SparkConf sc = new SparkConf().setMaster("local[*]");
	 SparkSession spark = SparkSession
	   .builder()
	   .config(sc)
	   .appName("LogisticRegressionTest")
	   .getOrCreate();
	
	 Dataset<Row> fullData = spark.read().csv("data/heart_disease_data.csv");
	 		fullData.createOrReplaceTempView("heartdiseasedata");
	 		fullData.show();
	 	 
	 Dataset<Row> selFeaturesdata = spark.sql("select _c0 age,_c1 sex,_c2 cp,_c3 sqft_lot, _c4 price,_c13 has_disease from heartdiseasedata");

	 selFeaturesdata.printSchema();

	 JavaRDD<Row> vectorsData = selFeaturesdata.javaRDD().map(s -> {
		 //System.out.println(s.getString(1) + " , " + s.getString(0));
		 return RowFactory.create((Double.parseDouble(s.getString(5).trim()) > 0) ? 1.0 : 0.0,
				 	Vectors.dense( Double.parseDouble(s.getString(0).trim()),
				 				   Double.parseDouble(s.getString(1).trim()),
				 				   Double.parseDouble(s.getString(2).trim()),
				 				   Double.parseDouble(s.getString(3).trim()),
				 				   Double.parseDouble(s.getString(4).trim()))
				 	);
	 });
	 
	 StructType schema = new StructType(new StructField[]{
			    new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
			    new StructField("features", new VectorUDT(), false, Metadata.empty())
			});
	 
	 Dataset<Row> trn = spark.createDataFrame(vectorsData, schema);
	 
	 trn.show();
	 
	// Prepare training and test data.
	 Dataset<Row>[] splits = trn.randomSplit(new double[] {0.9, 0.1}, 12345);
	 Dataset<Row> trainingData = splits[0];
	 Dataset<Row> testData = splits[1];
	 
	 LogisticRegression lr = new LogisticRegression();
	 	lr.setMaxIter(30);
	 	//lr.setRegParam(0.5);
	 	lr.setElasticNetParam(0.2);
	 
	
	 // Fit the model.
	 LogisticRegressionModel lrModel = lr.fit(trainingData);
	
	//  Print the coefficients and intercept for linear regression.
//
//	
//	//  Summarize the model over the training set and print out some metrics.
//	 LogisticRegressionTrainingSummary trainingSummary = lrModel.summary();
//	 //System.out.println("numIterations: " + trainingSummary.totalIterations());
//	 //System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
//	 //trainingSummary.residuals().show();
//	 System.out.println("Coefficients: "
//			   + lrModel.coefficients() + " Intercept: " + lrModel.intercept());	 
//	 System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
//	 //System.out.println("r2: " + trainingSummary.r2());
	  
	 
	 Dataset<Row> results = lrModel.transform(testData);
	 Dataset<Row> rows = results.select("features", "label", "prediction");
	 for (Row r: rows.collectAsList()) {
	   System.out.println("(" + r.get(0) + ", " + r.get(1) + ") " + ", prediction=" + r.get(2));
	 }
	 
	 int testDataLength = new Integer("" + rows.count());
	 System.out.println("testDataLength" + testDataLength);
//	 int wrongResultsCnt = 0;
//	 for (Row r: rows.collectAsList()) {
//		  if(r.getDouble(1) != r.getDouble(2)) wrongResultsCnt = wrongResultsCnt + 1;
//	 }
//	 System.out.println("wrongResultsCnt of wrong results --> " + wrongResultsCnt);
//	 double percentOfWrong = (wrongResultsCnt * 100)/testDataLength;
//	 System.out.println("Percent of wrong results --> " + percentOfWrong);
	 
//	 System.out.println("lr.getMaxIter() -->" + lr.getMaxIter());
//	 System.out.println("lr.getElasticNetParam() -->" + lr.getElasticNetParam());
//	 System.out.println("lr.getRegParam() -->" + lr.getRegParam());
	 spark.stop();
	}

}
