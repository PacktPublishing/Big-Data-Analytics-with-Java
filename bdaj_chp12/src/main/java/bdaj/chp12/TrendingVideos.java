package bdaj.chp12;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;

public class TrendingVideos {

	public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("kafka-sandbox")
                .setMaster("local[*]");
	    SparkSession spark = SparkSession
	  	      .builder()
	  	      .config(conf)
	  	      .appName("SentimentalAnalysisTest2")
	  	      .getOrCreate();
	    
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(10000));
       sc.setLogLevel("ERROR");

        Set<String> topics = Collections.singleton("test11");
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");

        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        JavaPairDStream<String, String> windowStream = directKafkaStream.window(new Duration(30000));
        windowStream.foreachRDD(rdd -> {
            	System.out.println("--- WINDOW ID --- " + rdd.id());
            	JavaRDD<VideoVO> videoRDD = rdd.map(s -> {
            		String[] rowStr = s._2.split(",");
        	    	VideoVO tvo = new VideoVO();
        	    		tvo.setVideoID(rowStr[0]);
        	    		tvo.setVideoCount(Integer.parseInt(rowStr[1]));
        	    		return tvo;
            	});
            	Dataset<Row> videoDS = spark.createDataFrame(videoRDD.rdd(), VideoVO.class);
            				 videoDS.createOrReplaceTempView("videos");
            				 
            				 spark.sql("select videoID,sum(videoCount) videoHitsCount from videos group by videoID order by videoHitsCount desc").show();
            	
            	
        	}
        );

        ssc.start();
        ssc.awaitTermination();

	}

}
