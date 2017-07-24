package chp9;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel; 
import org.jfree.chart.JFreeChart; 
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset; 
import org.jfree.data.category.DefaultCategoryDataset; 
import org.jfree.ui.ApplicationFrame; 
import org.jfree.ui.RefineryUtilities;

import chp3.vo.Car;
import chp8.LoanVO; 

public class RatingBarChart extends ApplicationFrame
{
	
	public static String APP_NAME = "GradeCntBarChart";
	public static String APP_MASTER = "local";
	
   public RatingBarChart( String applicationTitle , String chartTitle )
   {
      super( applicationTitle );        
      JFreeChart barChart = ChartFactory.createBarChart(
         chartTitle,           
         "Movie Rating",            
         "Number of Movies",            
         createDataset(),          
         PlotOrientation.VERTICAL,           
         true, true, false);
         
      ChartPanel chartPanel = new ChartPanel( barChart );        
      chartPanel.setPreferredSize(new java.awt.Dimension( 560 , 367 ) );        
      setContentPane( chartPanel ); 
   }
//   private CategoryDataset createDataset( )
//   {
//      final String fiat = "FIAT";        
//      final String audi = "AUDI";        
//      final String ford = "FORD";        
//      final String speed = "Speed";        
//      final String millage = "Millage";        
//      final String userrating = "User Rating";        
//      final String safety = "safety";        
//      final DefaultCategoryDataset dataset = 
//      new DefaultCategoryDataset( );  
//
//      dataset.addValue( 1.0 , fiat , speed );        
//      dataset.addValue( 3.0 , fiat , userrating );        
//      dataset.addValue( 5.0 , fiat , millage ); 
//      dataset.addValue( 5.0 , fiat , safety );           
//
//      dataset.addValue( 5.0 , audi , speed );        
//      dataset.addValue( 6.0 , audi , userrating );       
//      dataset.addValue( 10.0 , audi , millage );        
//      dataset.addValue( 4.0 , audi , safety );
//
//      dataset.addValue( 4.0 , ford , speed );        
//      dataset.addValue( 2.0 , ford , userrating );        
//      dataset.addValue( 3.0 , ford , millage );        
//      dataset.addValue( 6.0 , ford , safety );               
//
//      return dataset; 
//   }
   
   
   private CategoryDataset createDataset() {
	    String category = "Movie ratings";
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

	    final DefaultCategoryDataset dataset = 
	    	      new DefaultCategoryDataset( );  	    
		List<Row> results = ratingsGrps.collectAsList();
		for (Row row : results) {
			String key = "" + row.getFloat(0);
			dataset.addValue( row.getLong(1) , category , key );
		}
		return dataset;
   }
   public static void main( String[ ] args )
   {
	  RatingBarChart chart = new RatingBarChart("Zip code with max defaulted loans", "Top ten zipcodes with max defaulted loans.");
      chart.pack( );        
      RefineryUtilities.centerFrameOnScreen( chart );        
      chart.setVisible( true ); 
   }
}
