package big_data_analytics_java.chp6;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

// Word Count
public class SentiExporation2 {

	public static void main(String[] args) {
		SparkConf c = new SparkConf().setMaster("local");
	    
		SparkSession spark = SparkSession
	      .builder()
	      .config(c)
	      .appName("LoanLendingTree")
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
	    
	    Tokenizer tokenizer = new Tokenizer().setInputCol("tweet").setOutputCol("words");
	    
	    Dataset<Row> dataDs = tokenizer.transform(tweetsDs);
	    
	    JavaRDD<Object> words = dataDs.select("words").javaRDD().flatMap(s -> s.getList(0).iterator() );
//	    List<Object> f = words.take(5);
//	    for (Object object : f) {
//			System.out.println(object.toString());
//		}
	    
	   // JavaRDD<String> words = tweetsRdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator() );
	    
	    JavaPairRDD<String, Integer> wpairs = words.mapToPair(w -> new Tuple2(w.toString(), 1) );
	    
	    JavaPairRDD<String, Integer> wcounts = wpairs.reduceByKey((x,y) -> x + y);
	    
	    JavaRDD<WordVO> wordsRdd = wcounts.map(x -> {
	    	WordVO vo = new WordVO();
	    		vo.setWord(x._1);
	    		vo.setCount(x._2);
	    		return vo;
	    });
	    
	    Dataset<Row> wordsDS = spark.createDataFrame(wordsRdd, WordVO.class);
	    			 wordsDS.createOrReplaceTempView("words");
	    Dataset<Row> topWords = spark.sql("select word,count from words order by count desc");
	    	topWords.show();
	    
	    
//	    //Top 20 words
//	    List<Tuple2<String, Integer>> ws = wcounts.take(100);
//	    
//	    for (Tuple2<String, Integer> tuple2 : ws) {
//			System.out.println("" + tuple2._1 + " , " + tuple2._2);
//		}
	    
	    
	}

}
