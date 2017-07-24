package chp6;

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

public class SentiExploreBarChart extends ApplicationFrame
{
	
	public static String APP_NAME = "BarChartExample";
	public static String APP_MASTER = "local";
	
   public SentiExploreBarChart( String applicationTitle , String chartTitle )
   {
      super( applicationTitle );        
      JFreeChart barChart = ChartFactory.createBarChart(
         chartTitle,           
         "Type of Sentiment",            
         "Sentiments Count",            
         createDataset(),          
         PlotOrientation.VERTICAL,           
         true, true, false);
         
      ChartPanel chartPanel = new ChartPanel( barChart );        
      chartPanel.setPreferredSize(new java.awt.Dimension( 560 , 367 ) );        
      setContentPane( chartPanel ); 
   }

   
   private CategoryDataset createDataset() {
	    String category = "sa";
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
	    tweetsDs.createOrReplaceTempView("tweets");
    	
    Dataset<Row> saCountDS = spark.sql("select label, count(*) from tweets group by label");
    
	    final DefaultCategoryDataset dataset = 
	    	      new DefaultCategoryDataset( );  	    
		List<Row> results = saCountDS.collectAsList();
		for (Row row : results) {
			String key = "" + row.getDouble(0);
			if(null == key) key = "(Empty Values)";
			else if("1.0".equals(key))  key = "Positive";
			else key = "Negative";
			dataset.addValue( row.getLong(1) , category , key );
			System.out.println(row);
		}
		
		return dataset;
   }
   public static void main( String[ ] args )
   {
	   SentiExploreBarChart chart = new SentiExploreBarChart("Negative and Positive sentiments count.", "Negative and Positive sentiments count.");
      chart.pack( );        
      RefineryUtilities.centerFrameOnScreen( chart );        
      chart.setVisible( true ); 
   }
}

