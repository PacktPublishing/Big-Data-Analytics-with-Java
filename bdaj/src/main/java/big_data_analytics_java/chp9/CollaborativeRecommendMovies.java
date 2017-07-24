package big_data_analytics_java.chp9;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CollaborativeRecommendMovies {

	public static void main(String[] args) {
		SparkConf sconf = new SparkConf().setMaster("local[*]");
	    SparkSession spark = SparkSession
	    	      .builder()
	    	      .config(sconf)
	    	      .appName("CollaborativeRecommendMovies")
	    	      .getOrCreate();

	 	JavaRDD<MovieVO> movieRdd = spark
   				.read().textFile("data/movie/u.item").javaRDD()
   				.map(row -> {
   				String[] strs = row.split("\\|");
   				
   				MovieVO mvo = new MovieVO();
   				        mvo.setMovieId(strs[0]);
	      			    mvo.setMovieTitle(strs[1]);
 	    		   return mvo;
   				});	    
	 	Dataset<Row> movieDS = spark.createDataFrame(movieRdd.rdd(), MovieVO.class);
		 movieDS.createOrReplaceTempView("movies");	 	
		 
	    	    // $example on$
	    	    JavaRDD<RatingVO> ratingsRDD = spark
	    	      .read().textFile("data/movie/u.data").javaRDD()
	    	      .map(row -> {
	    	          String[] fields = row.split("\t");
	    	          if (fields.length != 4) {
	    	            return null;
	    	          }
	    	          int userId = Integer.parseInt(fields[0]);
	    	          int movieId = Integer.parseInt(fields[1]);
	    	          float rating = Float.parseFloat(fields[2]);
	    	          long timestamp = Long.parseLong(fields[3]);
	    	          
	    	         // if(rating > 3) return new RatingVO(userId, movieId, rating, timestamp,1);
	    	          return new RatingVO(userId, movieId, rating, timestamp);
	    	      }).filter(f -> f != null);
	    	    Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, RatingVO.class);
	    	    Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8, 0.2});
	    	    Dataset<Row> training = splits[0];
	    	    Dataset<Row> test = splits[1];

	    	    // Build the recommendation model using ALS on the training data
	    	    ALS als = new ALS()
	    	      .setMaxIter(10)
	    	      .setRegParam(0.01)
	    	      .setUserCol("userId")
	    	      .setItemCol("movieId")
	    	      .setRatingCol("rating");
	    	    
	    	    ALSModel model = als.fit(training);

	    	    // Evaluate the model by computing the RMSE on the test data
	    	    Dataset<Row> predictions = model.transform(test);
	    	    			 predictions.createOrReplaceTempView("predictions");
	    	    			 
	    	    spark.sql("select m.movieTitle,p.* from predictions p,movies m where p.userId = 633 and p.movieId = m.movieId order by p.prediction desc").show();
//	    	    predictions.show();
	    	    
//	    	    RegressionEvaluator evaluator = new RegressionEvaluator()
//	    	      .setMetricName("rmse")
//	    	      .setLabelCol("rating")
//	    	      .setPredictionCol("prediction");
//	    	    Double rmse = evaluator.evaluate(predictions);
//	    	    System.out.println("Root-mean-square error = " + rmse);
//	    	    // $example off$
	    	    spark.stop();
	}

}
