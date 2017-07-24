package bdaj.chp12;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;


public class SAOnTheFly {

	private static PipelineModel pm;
	
	public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("kafka-sandbox")
                .setMaster("local[*]");
	    SparkSession spark = SparkSession
	  	      .builder()
	  	      .config(conf)
	  	      .appName("SentimentalAnalysisTest2")
	  	      .getOrCreate();
        	
	    	trainModel(spark);
	    
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(10000));

        Set<String> topics = Collections.singleton("test11");
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");

        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        directKafkaStream.foreachRDD(rdd -> {
            	System.out.println("--- New RDD with " + rdd.partitions().size()
            						+ " partitions and " + rdd.count() + " records");
            	JavaRDD<TweetVO> tweetRdd = rdd.map(s -> {
            		String rowStr = s._2;
        	    	TweetVO tvo = new TweetVO();
        	    		tvo.setTweet(rowStr);
        	    		return tvo;
            	});
            	Dataset<Row> tweetsDs = spark.createDataFrame(tweetRdd.rdd(), TweetVO.class);
            	
            	tweetsDs.show();
            	
            	Dataset<Row> predictedResult = pm.transform(tweetsDs);
            	
            	System.out.println("####################### Predicted Result ###############################" );
            			predictedResult.show();
            			
            			//predictedResult.write().format("parquet").save(<HDFS LOCATIOn>);
            	System.out.println("######################################################" );
            	
            	
        	}
        );

        ssc.start();
        ssc.awaitTermination();

	}
	

	public static void trainModel(SparkSession spark) {
	    
	    String outputDir = "file:///C:/tmp/storage";
	    
	    JavaRDD<String> data = spark.sparkContext().textFile("file:///C:/harpreet/workspace-udemy/SparkExamples/data/sa/training.txt", 1).toJavaRDD();
	    
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
	    
	    pm = p.fit(training);
	        
	    //	pm.save(outputDir + "/c11");
	    	
	    	System.out.println("done done done");
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
