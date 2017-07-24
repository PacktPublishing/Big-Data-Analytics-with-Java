package big_data_analytics_java.chp9;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ContentBasedRecommender {

	public static void main(String[] args) {
		SparkConf sc = new SparkConf().setMaster("local");
	    SparkSession spark = SparkSession
	    	      .builder()
	    	      .config(sc)
	    	      .appName("ContentBasedRecommender")
	    	      .getOrCreate();

	    	    // Rating
	    	    JavaRDD<RatingVO> ratingsRDD = spark
	    	      .read().textFile("data/movie/u.data").javaRDD()
	    	      .map(row -> {
	    	    	 RatingVO rvo = RatingVO.parseRating(row);
	    	    	 	return rvo;
	    	      });
	    	    Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, RatingVO.class);
	    	    		     ratings.createOrReplaceTempView("ratings");
	    	    		     ratings.show();

	    	    Dataset<Row> moviesLikeCntDS = spark.sql("select movieId,avg(rating) averageRating from ratings group by movieId");
	    	    
	 	    	// Movie
	    	 	JavaRDD<MovieVO> movieRdd = spark
	    	 	    	      				.read().textFile("data/movie/u.item").javaRDD()
	    	 	    	      				.map(row -> {
	    	 	    	      				String[] strs = row.split("\\|");
	    	 	    	      				
	    	 	    	      				MovieVO mvo = new MovieVO();
	    	 	    	      				   mvo.setMovieId(strs[0]);
		    	 	    	      			   mvo.setMovieTitle(strs[1]);
			    	 	    	    		   mvo.setReleaseDate(strs[2]);
			    	 	    	    		  // mvo.setVideoReleaseDate(strs[3]);
			    	 	    	    		   mvo.setImdbUrl(strs[4]);
//			    	 	    	    		   mvo.setUnknown(strs[5]);
			    	 	    	    		   mvo.setAction(strs[6]);
			    	 	    	    		   mvo.setAdventure(strs[7]);
			    	 	    	    		   mvo.setAnimation(strs[8]);
			    	 	    	    		   mvo.setChildren(strs[9]);
			    	 	    	    		   mvo.setComedy(strs[10]);
			    	 	    	    		   mvo.setCrime(strs[11]);
			    	 	    	    		   mvo.setDocumentary(strs[12]);
			    	 	    	    		   mvo.setDrama(strs[13]);
			    	 	    	    		   mvo.setFantasy(strs[14]);
			    	 	    	    		   mvo.setFilmNoir(strs[15]);
			    	 	    	    		   mvo.setHorror(strs[16]);
			    	 	    	    		   mvo.setMusical(strs[17]);
			    	 	    	    		   mvo.setMystery(strs[18]);
			    	 	    	    		   mvo.setRomance(strs[19]);
			    	 	    	    		   mvo.setSciFi(strs[20]);
			    	 	    	    		   mvo.setThriller(strs[21]);
			    	 	    	    		   mvo.setWar(strs[22]);
			    	 	    	    		   mvo.setWestern(strs[23]);
			    	 	    	    		   return mvo;
	    	 	    	      				});
	    	 	Dataset<Row> movieDS = spark.createDataFrame(movieRdd.rdd(), MovieVO.class).join(moviesLikeCntDS, "movieId");
	    	 				 movieDS.createOrReplaceTempView("movies");
	    	 				 movieDS.show();
	    	    
	    	 	Dataset<Row> movieDataDS = 
	    	 			spark.sql("select m.movieId movieId1,m.movieTitle movieTitle1,m.action action1,m.adventure adventure1,"
	    	 						+ "m.animation animation1,m.children children1,m.comedy comedy1,m.crime crime1,m.documentary documentary1,"
	    	 						+ "m.drama drama1, m.fantasy fantasy1,m.filmNoir filmNoir1,m.horror horror1,m.musical musical1,m.mystery mystery1,"
	    	 						+ "m.romance romance1,m.sciFi sciFi1,m.thriller thriller1,m.war war1,m.western western1,m.averageRating avgRating1,"
	    	 						+ "m2.movieId movieId2,m2.movieTitle movieTitle2,m2.action action2,m2.adventure adventure2,"
	    	 						+ "m2.animation animation2,m2.children children2,m2.comedy comedy2,m2.crime crime2,m2.documentary documentary2,"
	    	 						+ "m2.drama drama2, m2.fantasy fantasy2,m2.filmNoir filmNoir2,m2.horror horror2,m2.musical musical2,m2.mystery mystery2,"
	    	 						+ "m2.romance romance2,m2.sciFi sciFi2,m2.thriller thriller2,m2.war war2,m2.western western2,m2.averageRating averageRating2 "
	    	 						+ "from movies m, movies m2 where m.movieId != m2.movieId");
	    	 				
	    	 				//movieDataDS.show();
	    	 					
	    	 	//euclid distance
	    	 	JavaRDD<EuclidVO> euclidRdd = movieDataDS.javaRDD().map( row -> {
	    	 		EuclidVO evo = new EuclidVO();
	    	 				 evo.setMovieId1(row.getString(0));
	    	 				 evo.setMovieTitle1(row.getString(1));
	    	 				 evo.setMovieTitle2(row.getString(22));
	    	 				 evo.setMovieId2(row.getString(21));
	    	 		int action = Math.abs(Integer.parseInt(row.getString(2)) - Integer.parseInt(row.getString(23)) );
	    	 		int adventure =Math.abs(Integer.parseInt(row.getString(3)) - Integer.parseInt(row.getString(24)) );
	    	 		int animation = Math.abs(Integer.parseInt(row.getString(4)) - Integer.parseInt(row.getString(25)) );
	    	 		int children = Math.abs(Integer.parseInt(row.getString(5)) - Integer.parseInt(row.getString(26)) );
	    	 		int comedy = Math.abs(Integer.parseInt(row.getString(6)) - Integer.parseInt(row.getString(27)) );
	    	 		int crime = Math.abs(Integer.parseInt(row.getString(7)) - Integer.parseInt(row.getString(28)) );
	    	 		int documentary = Math.abs(Integer.parseInt(row.getString(8)) - Integer.parseInt(row.getString(29)) );
	    	 		int drama = Math.abs(Integer.parseInt(row.getString(9)) - Integer.parseInt(row.getString(30)) );
	    	 		int fantasy = Math.abs(Integer.parseInt(row.getString(10)) - Integer.parseInt(row.getString(31)) );
	    	 		int filmNoir = Math.abs(Integer.parseInt(row.getString(11)) - Integer.parseInt(row.getString(32)) );
	    	 		int horror = Math.abs(Integer.parseInt(row.getString(12)) - Integer.parseInt(row.getString(33)) );
	    	 		int musical = Math.abs(Integer.parseInt(row.getString(13)) - Integer.parseInt(row.getString(34)) );
	    	 		int mystery = Math.abs(Integer.parseInt(row.getString(14)) - Integer.parseInt(row.getString(35)) );
	    	 		int romance = Math.abs(Integer.parseInt(row.getString(15)) - Integer.parseInt(row.getString(36)) );
	    	 		int scifi = Math.abs(Integer.parseInt(row.getString(16)) - Integer.parseInt(row.getString(37)) );
	    	 		int thriller = Math.abs(Integer.parseInt(row.getString(17)) - Integer.parseInt(row.getString(38)) );
	    	 		int war = Math.abs(Integer.parseInt(row.getString(18)) - Integer.parseInt(row.getString(39)) );
	    	 		int western = Math.abs(Integer.parseInt(row.getString(19)) - Integer.parseInt(row.getString(40)) );
	    	 		double likesCnt = Math.abs(row.getDouble(20) - row.getDouble(41) );
	    	 		
	    	 		double euclid = Math.sqrt(action * action + adventure * adventure + animation * animation + children * children + 
	    	 							   comedy * comedy + crime * crime + documentary * documentary + drama * drama + 
	    	 							   fantasy * fantasy + filmNoir * filmNoir + horror * horror + musical * musical + mystery * mystery + romance * romance + 
	    	 							   scifi * scifi + thriller * thriller + war * war + western * western + likesCnt * likesCnt);
	    	 	   evo.setEuclidDist(euclid);
	    	 	   return evo;
	    	 	});
	    	 	
	    	 	Dataset<Row> results = spark.createDataFrame(euclidRdd.rdd(), EuclidVO.class);
	    	 				 results.createOrReplaceTempView("movieEuclids");
	    	 				 
	    	 				 spark.sql("select * from movieEuclids where movieId1 = 1 order by euclidDist asc").show(20);
	    	 	
	    	 				 	
	    	    spark.stop();
	}

}
