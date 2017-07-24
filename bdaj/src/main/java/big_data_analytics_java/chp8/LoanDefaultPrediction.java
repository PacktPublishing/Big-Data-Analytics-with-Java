package big_data_analytics_java.chp8;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LoanDefaultPrediction {
	
	public static void main(String[] args) {
		SparkConf c = new SparkConf().setMaster("local[*]");
	    
		SparkSession spark = SparkSession
								.builder()
								.config(c)
								.appName("LoanDefaultPrediction")
								.getOrCreate();
	    
		
	    Dataset<Row> defaultData = spark.read().csv("C:/harpreet/datasets/LoanStats3a.csv");
		 // defaultData.show();

JavaRDD<Row> rdd = defaultData.toJavaRDD();

JavaRDD<LoanVO> data = rdd.map( r -> {
	if(r.size() < 100) return null;
	LoanVO lvo = new LoanVO();
	String loanId = r.getString(0).trim();
//	System.out.println("loanId --> " + loanId);
	String loanAmt = r.getString(2).trim();
//	System.out.println("loanId --> " + loanId);;
//	System.out.println("localid --> " + loanAmt.trim());;
//	System.out.println("loanId --> " + loanId);
	String fundedAmt = r.get(3).toString().trim();
	String grade = r.get(8).toString().trim();
	String subGrade = r.get(9).toString().trim();
	String empLength = r.get(11).toString().trim();
	String homeOwn = r.get(12).toString().trim();
	String annualInc = r.getString(13);
	String loanStatus = r.get(16).toString().trim();
				
	if(null == annualInc || "".equals(annualInc) || 
			null == loanAmt || "".equals(loanAmt) || 
			null == grade || "".equals(grade) || 
			null == subGrade || "".equals(subGrade) || 
			null == empLength || "".equals(empLength) || 
			null == homeOwn || "".equals(homeOwn) || 
			null == loanStatus || "".equals(loanStatus)) return null;
	
	
	if(loanAmt.contains("N/A") || loanId.contains("N/A") || fundedAmt.contains("N/A") || grade.contains("N/A") ||
			subGrade.contains("N/A") || empLength.contains("N/A") || homeOwn.contains("N/A") || annualInc.contains("N/A") || loanStatus.contains("N/A")) 
		return null;
	
	
	
	if("Current".equalsIgnoreCase(loanStatus)) return null;
	
		   lvo.setLoanAmt(Double.parseDouble(loanAmt));
		   lvo.setLoanId(Integer.parseInt(loanId));
		   lvo.setFundedAmt(Double.parseDouble(fundedAmt));
	//	   lvo.setFundedAmtInv(Double.parseDouble(darr[4].trim()));
		   lvo.setGrade(grade);
		   lvo.setSubGrade(subGrade);
		   lvo.setEmpLengthStr(empLength);
		   lvo.setHomeOwnership(homeOwn);
		   lvo.setAnnualInc(Double.parseDouble(annualInc.trim()));
		   
		   lvo.setLoanStatusStr(loanStatus);
		   
		   if(loanStatus.contains("Fully")) lvo.setLoanStatus(1.0);
		   else lvo.setLoanStatus(0.0);
		   
	//	   lvo.setLoanDesc(darr[20].trim());
	//	   lvo.setTitle(darr[22].trim());
	//	   lvo.setZipCode(darr[23].trim());
		   
		   return lvo;
	
	} ).filter(f ->  {
	if(f == null) return false;
	else return true;
	});
	
Dataset<Row> dataDS = spark.createDataFrame(data.rdd(), LoanVO.class);
			 dataDS.createOrReplaceTempView("loans");

	    		//dataDS.show();
	    			 
	    			// dataDS.describe("loanAmt","fundedAmt","homeOwnership").show();
	    		
	    
	    
	    //2. Counting loans by grades
//	    Dataset<Row> loansByGrade = spark.sql("select subGrade,count(*) from loans group by subGrade");
//	    			 loansByGrade.show();
	    			 
	 //   Dataset<Row> grpByGrade = spark.sql("select grade,count(*) from 

	    		
	    //3. Checking the distinc loan statues
//	    Dataset<Row> distinctLoanStatuses = spark.sql("select distinct loanStatusStr from loans");
//	    	distinctLoanStatuses.show();
//	    	
//	    //4. loan status count
//		    //3. Checking the distinc loan statues
//		Dataset<Row> grpLoanStatusCnt = spark.sql("select loanStatus,count(*) from loans group by loanStatus");
//		grpLoanStatusCnt.show();
	    		
	    		///////////////////////////
	    		
	    	    // Index labels, adding metadata to the label column.
	    	    // Fit on whole dataset to include all labels in index.
	    	    StringIndexerModel labelIndexer = new StringIndexer()
	    	      .setInputCol("loanStatus")
	    	      .setOutputCol("indexedLabel")
	    	      .fit(dataDS);

	    	    StringIndexerModel gradeIndexer = new StringIndexer()
	  	    	      .setInputCol("grade")
	  	    	      .setOutputCol("gradeLabel")
	  	    	      .fit(dataDS);
	    	    
	    	    StringIndexerModel homeIndexer = new StringIndexer()
	  	    	      .setInputCol("homeOwnership")
	  	    	      .setOutputCol("homeOwnershipLabel")
	  	    	      .fit(dataDS);
	    	    
	    	    
	    	    String[] featuresArr = {"loanAmt","annualInc","fundedAmt","gradeLabel","homeOwnershipLabel"};
	    	    VectorAssembler va = new VectorAssembler().setInputCols(featuresArr).setOutputCol("features");	    	    

	    	    // Split the data into training and test sets (30% held out for testing)
	    	    Dataset<Row>[] splits = dataDS.randomSplit(new double[] {0.8, 0.2});
	    	    Dataset<Row> trainingData = splits[0];
	    	    Dataset<Row> testData = splits[1];

	    	    // Train a RandomForest model.
	    	    RandomForestClassifier rf = new RandomForestClassifier()
	    	      .setLabelCol("indexedLabel")
	    	      .setFeaturesCol("features").setImpurity("entropy").setNumTrees(15);

	    	    // Convert indexed labels back to original labels.
	    	    IndexToString labelConverter = new IndexToString()
	    	      .setInputCol("prediction")
	    	      .setOutputCol("predictedLabel")
	    	      .setLabels(labelIndexer.labels());

	    	    // Chain indexers and forest in a Pipeline
	    	    Pipeline pipeline = new Pipeline()
	    	      .setStages(new PipelineStage[] {labelIndexer,gradeIndexer,homeIndexer, va, rf, labelConverter});

	    	    // Train model. This also runs the indexers.
	    	    PipelineModel model = pipeline.fit(trainingData);

	    	    // Make predictions.
	    	    Dataset<Row> predictions = model.transform(testData);
	    	    predictions.show();
//	    	    // Select example rows to display.
	    	    predictions.select("predictedLabel", "loanStatus", "features").show(20);

	    	    // Select (prediction, true label) and compute test error
	    	    MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
	    	      .setLabelCol("indexedLabel")
	    	      .setPredictionCol("prediction")
	    	      .setMetricName("accuracy");
	    	    double accuracy = evaluator.evaluate(predictions);
	    	    System.out.println("Accuracy = " + (100 * accuracy));
	    	    System.out.println("Test Error = " + (1.0 - accuracy));

	    	  //  RandomForestClassificationModel rfModel = (RandomForestClassificationModel)(model.stages()[2]);
	    	  //  System.out.println("Learned classification forest model:\n" + rfModel.toDebugString());
	    		
	}
	
}
