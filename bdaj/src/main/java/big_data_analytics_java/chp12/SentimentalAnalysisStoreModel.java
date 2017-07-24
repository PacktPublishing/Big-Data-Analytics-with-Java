package big_data_analytics_java.chp12;

import java.io.IOException;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;


public class SentimentalAnalysisStoreModel {

	public static void main(String[] args) throws IOException {
		SparkConf c = new SparkConf().setMaster("local[*]");
	    SparkSession spark = SparkSession
	      .builder()
	      .config(c)
	      .appName("SentimentalAnalysisTest")
	      .getOrCreate();
	   
	    JavaRDD<String> data = spark.sparkContext().textFile("data/sa/training.txt", 1).toJavaRDD();
	    
	    JavaRDD<TweetVO> tweetsRdd = data.map(strRow -> {
	    	//System.out.println(strRow);
	    	String[] rowArr = strRow.split("\t");
	    	String rawTweet = rowArr[1];
	    	String realTweet = rawTweet.replaceAll(",", "").replaceAll("\"", "").replaceAll("\\*", "").replaceAll("\\.", "").trim();
	    	TweetVO tvo = new TweetVO();
	    		tvo.setTweet(realTweet);
	    		tvo.setLabel(Double.parseDouble(rowArr[0]));
	    		return tvo;
	    });
	    
	    
	    
	    Dataset<Row> tweetsDs = spark.createDataFrame(tweetsRdd.rdd(), TweetVO.class);
	    	tweetsDs.show();
	    Dataset<Row>[] tweetsDsArr = tweetsDs.randomSplit(new double[]{0.8,0.2});
	    Dataset<Row> training = tweetsDsArr[0];
	    Dataset<Row> testing = tweetsDsArr[1];
	    
	    Tokenizer tokenizer = new Tokenizer().setInputCol("tweet").setOutputCol("words");
	    
	    StopWordsRemover stopWrdRem = new StopWordsRemover().setInputCol("words").setOutputCol("updatedWords");
	    
	    int numFeatures = 10000;
	    HashingTF hashingTF = new HashingTF()
	    						.setInputCol("updatedWords")
	    						.setOutputCol("rawFeatures")
	    						.setNumFeatures(numFeatures);	
	    
	    IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
	    
	    NaiveBayes nb = new NaiveBayes().setFeaturesCol("features").setPredictionCol("predictions");
	    
	    Pipeline p = new Pipeline();
	    
	    	p.setStages(new PipelineStage[]{ tokenizer, stopWrdRem, hashingTF, idf,nb}); //, stopWrdRem, hashingTF, idf, nb
	    
	    PipelineModel pm = p.fit(training);
	    	pm.save("model/sa1");
//	    Dataset<Row> updTweetsDS = pm.transform(testing);
//	    	updTweetsDS.show();
//	    
//	        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
//	        	      .setLabelCol("label")
//	        	      .setPredictionCol("predictions")
//	        	      .setMetricName("accuracy");
//	        	    double accuracy = evaluator.evaluate(updTweetsDS);
//	        	    System.out.println("Accuracy = " + accuracy);
//	        	    System.out.println("Test Error = " + (1.0 - accuracy));	    
		
	}

}
