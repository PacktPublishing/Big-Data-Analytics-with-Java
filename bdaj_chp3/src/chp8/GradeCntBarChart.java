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
import chp8.LoanVO; 

public class GradeCntBarChart extends ApplicationFrame
{
	
	public static String APP_NAME = "GradeCntBarChart";
	public static String APP_MASTER = "local";
	
   public GradeCntBarChart( String applicationTitle , String chartTitle )
   {
      super( applicationTitle );        
      JFreeChart barChart = ChartFactory.createBarChart(
         chartTitle,           
         "Grade",            
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
	    String category = "loans category";
		SparkConf c = new SparkConf().setMaster("local");
	    
		SparkSession spark = SparkSession
	      .builder()
	      .config(c)
	      .appName("LoanLendingTree")
	      .getOrCreate();
	    
	    Dataset<Row> defaultData = spark.read().csv("C:/harpreet/datasets/LoanStats3a.csv");
		 // defaultData.show();

	    JavaRDD<Row> rdd = defaultData.toJavaRDD();

	    JavaRDD<LoanVO> data = rdd.map( r -> {
			if(r.size() < 100) return null;
			LoanVO lvo = new LoanVO();
			String loanId = r.getString(0).trim();
		//	System.out.println("loanId --> " + loanId);
			String loanAmt = r.getString(2).trim();
		//	System.out.println("loanId --> " + loanId);;
		//	System.out.println("localid --> " + loanAmt.trim());;
		//	System.out.println("loanId --> " + loanId);
			String fundedAmt = r.get(3).toString().trim();
			String grade = r.get(8).toString().trim();
			String subGrade = r.get(9).toString().trim();
			String empLength = r.get(11).toString().trim();
			String homeOwn = r.get(12).toString().trim();
			String annualInc = r.getString(13);
			String loanStatus = r.get(16).toString().trim();
				
			if(null == annualInc || "".equals(annualInc) || 
				null == loanAmt || "".equals(loanAmt) || 
				null == grade || "".equals(grade) || 
				null == subGrade || "".equals(subGrade) || 
				null == empLength || "".equals(empLength) || 
				null == homeOwn || "".equals(homeOwn) || 
				null == loanStatus || "".equals(loanStatus)) return null;
	
	
			if(loanAmt.contains("N/A") || loanId.contains("N/A") || fundedAmt.contains("N/A") || grade.contains("N/A") ||
					subGrade.contains("N/A") || empLength.contains("N/A") || homeOwn.contains("N/A") || annualInc.contains("N/A") || loanStatus.contains("N/A")) 
					return null;
	
	
	
			if("Current".equalsIgnoreCase(loanStatus)) return null;
	
			   lvo.setLoanAmt(Double.parseDouble(loanAmt));
			   lvo.setLoanId(Integer.parseInt(loanId));
			   lvo.setFundedAmt(Double.parseDouble(fundedAmt));
		//	   lvo.setFundedAmtInv(Double.parseDouble(darr[4].trim()));
			   lvo.setGrade(grade);
			   lvo.setSubGrade(subGrade);
			   lvo.setEmpLengthStr(empLength);
			   lvo.setHomeOwnership(homeOwn);
			   lvo.setAnnualInc(Double.parseDouble(annualInc.trim()));
			   
			   lvo.setLoanStatusStr(loanStatus);
		   
			   if(loanStatus.contains("Fully")) lvo.setLoanStatus(1.0);
			   else lvo.setLoanStatus(0.0);
		   	   
		   return lvo;
	
	} ).filter(f ->  {
		if(f == null) return false;
			else return true;
	});
	
	    Dataset<Row> dataDS = spark.createDataFrame(data.rdd(), LoanVO.class);
			 dataDS.createOrReplaceTempView("loans");
	    			 
	    			 
	    			 
	    			 Dataset<Row> loanAmtDS = spark.sql("select grade,count(*) from loans where loanStatus = 0.0 group by grade");
	    loanAmtDS.show();
	    final DefaultCategoryDataset dataset = 
	    	      new DefaultCategoryDataset( );  	    
		List<Row> results = loanAmtDS.collectAsList();
		for (Row row : results) {
			String key = row.getString(0);
			dataset.addValue( row.getLong(1) , category , key );
		}
		return dataset;
   }
   public static void main( String[ ] args )
   {
	  GradeCntBarChart chart = new GradeCntBarChart("Bad loans by grade", "Bad loans count categoried by grade.");
      chart.pack( );        
      RefineryUtilities.centerFrameOnScreen( chart );        
      chart.setVisible( true ); 
   }
}
