package chp11;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
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

public class DegreesBarChart extends ApplicationFrame
{
	
	public static String APP_NAME = "GradeCntBarChart";
	public static String APP_MASTER = "local";
	
   public DegreesBarChart( String applicationTitle , String chartTitle )
   {
      super( applicationTitle );        
      JFreeChart barChart = ChartFactory.createBarChart(
         chartTitle,           
         "State",            
         "Number of flights(in and out)",            
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
	    String category = "count of flights";
		SparkConf conf = new SparkConf().setMaster("local[*]");
		SparkSession session = SparkSession
								.builder()
								.config(conf)
							    .appName("FA")
							    .getOrCreate();
		
		//Airports
		Dataset<Row> rawDataAirport = session.read().csv("data/flight/airports.dat");
		rawDataAirport.show();
		
		JavaRDD<Airport> airportsRdd = 
				rawDataAirport.javaRDD().map(row -> {
					Airport ap = new Airport();
						ap.setAirportId(row.getString(0));
						ap.setState(row.getString(2));
						ap.setCountry(row.getString(3));
						ap.setAirportIataCode(row.getString(4));
						ap.setAirportIcaoCode(row.getString(5));
						ap.setAirportName(row.getString(1));
						ap.setLatitude(row.getString(6));
						ap.setLongitude(row.getString(7));
						ap.setId(row.getString(4));
					return ap;
				});
	
		Dataset<Row> airports = session.createDataFrame(airportsRdd.rdd(), Airport.class);
					 airports.createOrReplaceTempView("airports");
		
			airports.show();
			
		//Routes
		Dataset<Row> rawDataRoute = session.read().csv("data/flight/routes.dat");

		
			JavaRDD<Route> routesRdd = 
					rawDataRoute.javaRDD().map(row -> {
						Route r = new Route();
							r.setAirLineCode(row.getString(1));
//							r.setAirlineId(String)
							//r.setSrcId(String)
							r.setSrc(row.getString(2));
							//r.setDstId(String)
							r.setDst(row.getString(4));
//							r.setSrc(String)
//							r.setDst(String)
//							r.setRelationType(String)
							return r;
					});
		
			Dataset<Row> routes = session.createDataFrame(routesRdd.rdd(), Route.class);
			
			routes.show();	
			
		GraphFrame gf = new GraphFrame(airports, routes);
		
			gf.vertices().show();

			//1. Check graphframe is good setup
			gf.vertices().filter("country = 'US'").show();
			
			//2. Number of airports in india
			//System.out.println("Airports in USA ----> " + gf.vertices().filter("country = 'United States'").count());
			
			
			gf.degrees().createOrReplaceTempView("degrees");
			
		Dataset<Row> dataByCtryCnt = session.sql("select a.State, d.degree from airports a, degrees d where a.airportIataCode = d.id order by d.degree desc limit 10");
		
	    final DefaultCategoryDataset dataset = 
	    	      new DefaultCategoryDataset( );  	    
		List<Row> results = dataByCtryCnt.collectAsList();
		for (Row row : results) {
			String key = "" + row.getString(0);
			dataset.addValue( row.getInt(1) , category , key );
		}
		return dataset;
   }
   public static void main( String[ ] args )
   {
	  DegreesBarChart chart = new DegreesBarChart("Number of flights (in and out) vs Airport State", "Number of flights (in and out) vs Airport State");
      chart.pack( );        
      RefineryUtilities.centerFrameOnScreen( chart );        
      chart.setVisible( true ); 
   }
}
