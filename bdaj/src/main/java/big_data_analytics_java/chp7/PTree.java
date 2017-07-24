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

public class PTree {

	public static void main(String[] args) {
		SparkConf c = new SparkConf().setMaster("local");
	    SparkSession spark = SparkSession
	      .builder()
	      .config(c)
	      .appName("LoanLendingTree")
	      .getOrCreate();
	    Dataset<Row> rowDS = spark.read().csv("data/loan/loan_train.csv");
	    			 rowDS.createOrReplaceTempView("loans");
	    Dataset<Row> rowAvgDSIncome = spark.sql("select avg(_c6) avgIncome from loans");
	    rowAvgDSIncome.show();
	    Dataset<Row> rowAvgDSAmt = spark.sql("select avg(_c8) avgLoanAmount from loans");
	    	rowAvgDSAmt.show();	    		     
	    Row avgIncRow = rowAvgDSIncome.collectAsList().get(0);
	    Row avgLoanRow = rowAvgDSAmt.collectAsList().get(0);
	    double avgCreditHistory = 1.0;
	    
	    JavaRDD<String> rowRdd = 
	    		spark.sparkContext().textFile("data/loan/loan_train.csv", 1).toJavaRDD();
	    
	    JavaRDD<LoanVO> loansRdd = rowRdd.map( row -> {
	    	String[] dataArr = row.split(",");
	    	LoanVO lvo = new LoanVO();	
	    	if(dataArr[1].equals("Male")) lvo.setGender(1.0);
	    	else lvo.setGender(0.0);

	    	if(dataArr[2].equals("Married")) lvo.setMarried(1.0);
	    	else lvo.setGender(0.0);

	    	if(dataArr[4].equals("Graduate")) lvo.setEducation(1.0);
	    	else lvo.setGender(0.0);

	    	//System.out.println(dataArr[6]);
	    	if(null == dataArr[6] || "".equals(dataArr[6])) 
	    		lvo.setApplicantIncome(avgIncRow.getDouble(0));
	    	else lvo.setApplicantIncome(Double.parseDouble(dataArr[6]));	    	
	    	
	    	
	    	if(null == dataArr[8] || "".equals(dataArr[8])) 
	    		lvo.setLoanAmount(avgLoanRow.getDouble(0));
	    	else lvo.setLoanAmount(Double.parseDouble(dataArr[8]));
	    	
//	    	if(null == dataArr[9] || "".equals(dataArr[9])) 
//	    		lvo.setLoanAmountTerm(avgRow.getDouble(2));
//	    	else lvo.setLoanAmountTerm(Double.parseDouble(dataArr[9]));	    	
	    	
	    	
	    	if(null == dataArr[10] || "".equals(dataArr[10])) 
	    		lvo.setCreditHistory(avgCreditHistory);
	    	else lvo.setCreditHistory(Double.parseDouble(dataArr[10]));	    	
	    	
	    	if(dataArr[12].equals("Y")) lvo.setLoanStatus(1.0);
	    	else lvo.setLoanStatus(0.0);
	    	
	    	return lvo;
	    	
	    });
	    
	    Dataset<Row> dataDS = spark.createDataFrame(loansRdd.rdd(), LoanVO.class);
	    	
//	    	dataDS.printSchema();
	    
	    StringIndexerModel labelIndexer = new StringIndexer()
	    	      .setInputCol("loanStatus")
	    	      .setOutputCol("result")
	    	      .fit(dataDS);
	    
	    
	    
	    String[] featuresArr = {"loanAmount","applicantIncome","creditHistory"};
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
	        predictions.select("predictedLabel", "result", "features").show(10);
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

