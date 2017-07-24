package chp7;

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

public class BarChartExample extends ApplicationFrame
{
	
	public static String APP_NAME = "BarChartExample";
	public static String APP_MASTER = "local";
	
   public BarChartExample( String applicationTitle , String chartTitle )
   {
      super( applicationTitle );        
      JFreeChart barChart = ChartFactory.createBarChart(
         chartTitle,           
         "Credit History",            
         "Number of Loans",            
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
	    String category = "loans";
		SparkConf c = new SparkConf().setMaster("local");
		
	    SparkSession spark = SparkSession
	      .builder()
	      .config(c)
	      .appName("LoanApproval")
	      .getOrCreate();
	    Dataset<Row> rowDS = spark.read().csv("data/loan_train.csv");
	    			 rowDS.createOrReplaceTempView("loans");
	    Dataset<Row> loanAmtDS = spark.sql("select _c10,count(*) from loans where _c12 = 'Y' group by _c10");
	    loanAmtDS.show();
	    final DefaultCategoryDataset dataset = 
	    	      new DefaultCategoryDataset( );  	    
		List<Row> results = loanAmtDS.collectAsList();
		for (Row row : results) {
			String key = (null == row.getString(0))? "Empty Value":row.getString(0);
			if(null == row.getString(0)) key = "(Empty Values)";
			else if("1".equals(row.getString(0)))  key = "Good";
			else key = "Bad";
			dataset.addValue( row.getLong(1) , category , key );
			System.out.println(row);
		}
		
		return dataset;
   }
   public static void main( String[ ] args )
   {
	  BarChartExample chart = new BarChartExample("Approved Loan with Good/Bad credit history", "Which all loans were approved with good or bad credit history?");
      chart.pack( );        
      RefineryUtilities.centerFrameOnScreen( chart );        
      chart.setVisible( true ); 
   }
}
