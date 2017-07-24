package chp8;

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

public class SubGradeLoanChart extends ApplicationFrame
{
	
	public static String APP_NAME = "BarChartExample";
	public static String APP_MASTER = "local";
	
   public SubGradeLoanChart( String applicationTitle , String chartTitle )
   {
      super( applicationTitle );        
      JFreeChart barChart = ChartFactory.createBarChart(
         chartTitle,           
         "Grade of Loan",            
         "Number of Loans",            
         createDataset(),          
         PlotOrientation.VERTICAL,           
         true, true, false);
         
      ChartPanel chartPanel = new ChartPanel( barChart );        
      chartPanel.setPreferredSize(new java.awt.Dimension( 560 , 367 ) );        
      setContentPane( chartPanel ); 
   }

   
   private CategoryDataset createDataset() {
	    String category = "Loans Grade";
	    
		SparkConf c = new SparkConf().setMaster("local[*]");
	    
		SparkSession spark = SparkSession
	      .builder()
	      .config(c)
	      .appName("LoanLendingTree")
	      .getOrCreate();
	    
		
	    JavaRDD<String> dataRdd = 
	    		spark.sparkContext().textFile("C:/harpreet/datasets/LoanStats3a.csv",1).toJavaRDD();
	    
	    //1. For our Analysis we will filter all teh 'current' loans
	    JavaRDD<String> filteredLoans = dataRdd.filter( row -> {
	    	return !row.contains("Current");
	    });
	    
	    JavaRDD<LoanVO> data = filteredLoans.map( r -> {
	    	String[] darr = r.split(",");
	    	if(darr.length < 100) return null;
	    	LoanVO lvo = new LoanVO();
	    		   lvo.setLoanAmt(Double.parseDouble(darr[2].replaceAll("\"", "").trim()));
	    		   lvo.setLoanId(Integer.parseInt(darr[0].replaceAll("\"", "").trim()));
	    		   lvo.setFundedAmt(Double.parseDouble(darr[3].replaceAll("\"", "").trim()));
	    		   lvo.setFundedAmtInv(Double.parseDouble(darr[4].replaceAll("\"", "").trim()));
	    		   lvo.setGrade(darr[8].replaceAll("\"", "").trim());
	    		   lvo.setSubGrade(darr[9].replaceAll("\"", "").trim());
	    		   lvo.setEmpLengthStr(darr[11].replaceAll("\"", "").trim());
	    		   lvo.setHomeOwnership(darr[12].replaceAll("\"", "").trim());
	    		   lvo.setAnnualInc(Double.parseDouble(darr[2].replaceAll("\"", "").trim()));
	    		   String loanStatus = darr[16].replaceAll("\"", "").trim();
	    		   lvo.setLoanStatusStr(loanStatus);
	    		   
	    		   if(loanStatus.contains("Fully")) lvo.setLoanStatus(1.0);
	    		   else lvo.setLoanStatus(0.0);
	    		   
	    		   lvo.setLoanDesc(darr[20].replaceAll("\"", "").trim());
	    		   lvo.setTitle(darr[22].replaceAll("\"", "").trim());
	    		   lvo.setZipCode(darr[23].replaceAll("\"", "").trim());
	    		   
	    		   return lvo;

	    } ).filter(f ->  {
	    	if(f == null) return false;
	    	else return true;
	    });
	    	
	    Dataset<Row> dataDS = spark.createDataFrame(data.rdd(), LoanVO.class);
		 dataDS.createOrReplaceTempView("loans");

		 Dataset<Row> loansByGrade = spark.sql("select grade,count(*) from loans group by grade");
 			 loansByGrade.show();	    
	    final DefaultCategoryDataset dataset = 
	    	      new DefaultCategoryDataset( );  	    
		List<Row> results = loansByGrade.collectAsList();
		for (Row row : results) {
			String key = "" + row.getString(0);
			Long l = row.getLong(1);
			if(l != null)
				dataset.addValue( row.getLong(1) , category , key );
			//System.out.println(row);
		}
		
		return dataset;	    

   }
   public static void main( String[ ] args )
   {
	   SubGradeLoanChart chart = new SubGradeLoanChart("Count of loans by grade.", "Count of loans by grade.");
      chart.pack( );        
      RefineryUtilities.centerFrameOnScreen( chart );        
      chart.setVisible( true ); 
   }
}

