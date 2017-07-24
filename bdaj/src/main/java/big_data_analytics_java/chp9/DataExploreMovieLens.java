package big_data_analytics_java.chp9;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataExploreMovieLens {
   public static void main(String[] args) {
		SparkConf sconf = new SparkConf().setMaster("local[*]");
	    SparkSession spark = SparkSession
	    	      .builder()
	    	      .config(sconf)
	    	      .appName("DataExploreMovieLens")
	    	      .getOrCreate();

	    	    // $example on$
	    	    JavaRDD<RatingVO> ratingsRDD = spark
	    	      .read().textFile("data/movie/u.data").javaRDD()
	    	      .map(row -> {
	    	    	 RatingVO rvo = RatingVO.parseRating(row);
	    	    	 	return rvo;
	    	      });
	    	    Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, RatingVO.class);
	    	    	ratings.createOrReplaceTempView("ratings");
	    	    
	    	    // 1. Counting movie by ratings
	    	    Dataset<Row> ratingsGrps = spark.sql("select rating,count(*) from ratings group by rating");
	    	    	ratingsGrps.show();
   }
}
