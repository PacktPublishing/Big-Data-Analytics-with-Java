package big_data_analytics_java.chp7;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PTreeDiabetes {

	public static void main(String[] args) {
		SparkConf c = new SparkConf().setMaster("local");
	    SparkSession spark = SparkSession
	      .builder()
	      .config(c)
	      .appName("PimaIndianDiabTest")
	      .getOrCreate();
//	    Dataset<Row> rowDS = spark.read().csv("data/diabetes/pima_indian_diabetes.data");
//	    			 rowDS.createOrReplaceTempView("diabetes_data");
	    
	    JavaRDD<String> rowRdd = 
	    		spark.sparkContext().textFile("data/diabetes/pima_indian_diabetes.data", 1).toJavaRDD();

	    
	    JavaRDD<DataRowVO> diabRdd = rowRdd.map( row -> {
	    	String[] dataArr = row.split(",");
	    	DataRowVO dvo = new DataRowVO();	
	    		dvo.setNumPregCnt(Double.parseDouble(dataArr[0]));
	    		dvo.setPlasmaGlucConc(Double.parseDouble(dataArr[1]));
	    		dvo.setDiaBloodPressure(Double.parseDouble(dataArr[2]));
	    		dvo.setSkinFoldThickness(Double.parseDouble(dataArr[3]));
	    		dvo.setSerumInsulin(Double.parseDouble(dataArr[4]));
	    		dvo.setBodyMassIndex(Double.parseDouble(dataArr[5]));
	    		dvo.setDiabPedigreeFunc(Double.parseDouble(dataArr[6]));
	    		dvo.setAge(Double.parseDouble(dataArr[7]));
	    		dvo.setIsDiseasePresent(Double.parseDouble(dataArr[8]));
	    		return dvo;
	    	
	    });
	    
	    Dataset<Row> dataDS = spark.createDataFrame(diabRdd.rdd(), DataRowVO.class);
	    	
//	    	dataDS.printSchema();
	    
	    StringIndexerModel labelIndexer = new StringIndexer()
	    	      .setInputCol("isDiseasePresent")
	    	      .setOutputCol("result")
	    	      .fit(dataDS);
	    
	    String[] featuresArr = {"numPregCnt"};
	    //String[] featuresArr = {"loanAmountTerm"};
	    VectorAssembler va = new VectorAssembler().setInputCols(featuresArr).setOutputCol("features");
	    	va.transform(dataDS);
	    	


	        // Split the data into training and test sets (30% held out for testing).
	        Dataset<Row>[] splits = dataDS.randomSplit(new double[]{0.7, 0.3});
	        Dataset<Row> trainingData = splits[0];
	        Dataset<Row> testData = splits[1];

	        // Train a DecisionTree model.
	        DecisionTreeClassifier dt = new DecisionTreeClassifier()
	          .setLabelCol("result")
	          .setFeaturesCol("features").setImpurity("entropy");    	
	    	
	        IndexToString labelConverter = new IndexToString()
	        	      .setInputCol("prediction")
	        	      .setOutputCol("predictedLabel")
	        	      .setLabels(labelIndexer.labels());	    
	        
////////////////////
	        // Chain indexers and tree in a Pipeline.
	        Pipeline pipeline = new Pipeline()
	          .setStages(new PipelineStage[]{labelIndexer, va, dt, labelConverter});

	        // Train model. This also runs the indexers.
	        PipelineModel model = pipeline.fit(trainingData);

	        // Make predictions.
	        Dataset<Row> predictions = model.transform(testData);

	        // Select example rows to display.
	        predictions.select("predictedLabel", "result", "features").show(50);
///////////////////////////////
	        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
	        	      .setLabelCol("result")
	        	      .setPredictionCol("prediction")
	        	      .setMetricName("accuracy");
	        	    double accuracy = evaluator.evaluate(predictions);
	        	    System.out.println("Accuracy = " + accuracy);
	        	    System.out.println("Test Error = " + (1.0 - accuracy));

	}

}
