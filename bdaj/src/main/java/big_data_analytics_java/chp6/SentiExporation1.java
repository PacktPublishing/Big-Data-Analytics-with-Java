package big_data_analytics_java.chp6;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// Generic Details
// Count of Data
// COunt of number of positive and number of negative reviews
public class SentiExporation1 {

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
	    	String realTweet = rowArr[1];
	    	//String realTweet = rawTweet.replaceAll(",", "").replaceAll("\"", "").replaceAll("\\*", "").replaceAll("\\.", "").trim();
	    	TweetVO tvo = new TweetVO();
	    		tvo.setTweet(realTweet);
	    		tvo.setLabel(Double.parseDouble(rowArr[0]));
	    		return tvo;
	    });
	    
	    Dataset<Row> dataDS = spark.createDataFrame(tweetsRdd.rdd(), TweetVO.class);
	    			 dataDS.show(5);
	    	
//	    	//dataDS.describe("_c0");
//	    	//dataDS.show();
//	    	System.out.println(" Numbers of Rows --> " + tweetsRdd.count());
//	    	
//	    
	    	dataDS.createOrReplaceTempView("tweets");
//	    	
	    Dataset<Row> saCountDS = spark.sql("select label sentiment, count(*) from tweets group by label");
	    
	    		saCountDS.show(); // so more positive than negative reviews

	    
	    
	    	//rowDS.describe("_c0","_c1","_c2","_c3","_c4","_c5","_c6","_c7","_c8","_c9","_c10","_c11","_c12").show();

	}

}
