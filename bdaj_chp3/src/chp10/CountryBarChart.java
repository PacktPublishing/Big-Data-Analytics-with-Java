package chp10;

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

public class CountryBarChart extends ApplicationFrame
{
	
	public static String APP_NAME = "GradeCntBarChart";
	public static String APP_MASTER = "local";
	
   public CountryBarChart( String applicationTitle , String chartTitle )
   {
      super( applicationTitle );        
      JFreeChart barChart = ChartFactory.createBarChart(
         chartTitle,           
         "Country",            
         "Number of Data Points",            
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
	    String category = "barChart";
		SparkConf conf = new SparkConf().setMaster("local[*]");
		SparkSession session = SparkSession
								.builder()
								.config(conf)
							    .appName("Retail Data Exploration")
							    .getOrCreate();
		
		Dataset<Row> rawData = session.read().csv("data/retail/Online_Retail.csv");
		rawData.show();
		
		//only UK
		//Dataset<Row> rawDataUK = rawData.filter("_c7 = 'United Kingdom'");
		//	rawDataUK.show();
		
			JavaRDD<RetailVO> retailData = rawData.javaRDD().map(row -> {
				RetailVO retailVO = new RetailVO();
				// data cleanser
				// remove data with missing entries
				String invoiceNo = row.getString(0);
				String stockCode = row.getString(1);
				String description = row.getString(2);
				String quantity = row.getString(3);
				String invoiceDate = row.getString(4);
				String unitPrice = row.getString(5);
				String customerID = row.getString(6);
				String country = row.getString(7);
				
				if( null == invoiceNo || "".equals(invoiceNo.trim()) ||
				    null == stockCode || "".equals(stockCode.trim()) || 
				    null == description || "".equals(description.trim()) ||
				    null == quantity || "".equals(quantity.trim()) ||
				    null == invoiceDate || "".equals(invoiceDate.trim()) ||
				    null == unitPrice || "".equals(unitPrice.trim()) ||
				    null == customerID || "".equals(customerID.trim()) ||
				    null == country || "".equals(country.trim())) {
					return null;
				}
				
				retailVO.setInvoiceNo(invoiceNo);
				retailVO.setStockCode(stockCode);
				retailVO.setDescription(description);
				retailVO.setInvoiceDate(invoiceDate);
				retailVO.setUnitPrice( Double.parseDouble(unitPrice) );
				retailVO.setCustomerID(Integer.parseInt(customerID));
				if("United Kingdom".equals(country.trim())) retailVO.setCountry("UK");
				else retailVO.setCountry(country);
				
				return retailVO;
				
				//We will filter out any null value
			}).filter( rowObj -> null != rowObj);

		Dataset<Row> retailDS = session.createDataFrame(retailData.rdd(), RetailVO.class);
				retailDS.createOrReplaceTempView("retail");
				
		Dataset<Row> dataByCtryCnt = session.sql("select country,count(*) cnt from retail group by country having cnt > 1000");
		dataByCtryCnt.show();
		
	    final DefaultCategoryDataset dataset = 
	    	      new DefaultCategoryDataset( );  	    
		List<Row> results = dataByCtryCnt.collectAsList();
		for (Row row : results) {
			String key = "" + row.getString(0);
			dataset.addValue( row.getLong(1) , category , key );
		}
		return dataset;
   }
   public static void main( String[ ] args )
   {
	  CountryBarChart chart = new CountryBarChart("Number of data points by country", "Number of data items by country");
      chart.pack( );        
      RefineryUtilities.centerFrameOnScreen( chart );        
      chart.setVisible( true ); 
   }
}
