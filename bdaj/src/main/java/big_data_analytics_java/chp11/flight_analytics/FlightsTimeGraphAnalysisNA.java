package big_data_analytics_java.chp11.flight_analytics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Descending;
import org.graphframes.GraphFrame;
import org.graphframes.lib.PageRank;
import static org.apache.spark.sql.functions.*;

public class FlightsTimeGraphAnalysisNA {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]");
		SparkSession session = SparkSession
								.builder()
								.config(conf)
							    .appName("FA")
							    .getOrCreate();
	
		//Airlines
		Dataset<Row> rawDataAirline = session.read().csv("data/flight/airlines.dat");
		
		
		JavaRDD<Airline> airlinesRdd = 
				rawDataAirline.javaRDD().map(row -> {
					Airline ar = new Airline();
						ar.setAirlineId(Integer.parseInt(row.getString(0)));
						ar.setAirlineName(row.getString(1));
						ar.setAirlineCountry(row.getString(6));
						return ar;
				});
		Dataset<Row> airlines = session.createDataFrame(airlinesRdd.rdd(), Airline.class);
					 airlines.createOrReplaceTempView("airlines");;
		
		//Airports
		Dataset<Row> rawDataAirport = session.read().csv("data/flight/airports.dat");
		rawDataAirport.show();
		
		JavaRDD<Airport> airportsRdd = 
				rawDataAirport.javaRDD().map(row -> {
					
					if(!"United States".equals(row.getString(3))) return null;
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
				}).filter(r -> r != null);
	
		Dataset<Row> airports = session.createDataFrame(airportsRdd.rdd(), Airport.class);
					 airports.createOrReplaceTempView("airports");
		
			airports.show();
			
		//Routes
		//Dataset<Row> rawDataRoute = session.read().csv("data/flight/routes.dat");
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
			
			
			//Delay
				Dataset<Row> rawDataDelay= session.read().csv("data/flight/On_Time_On_Time_Performance_2017_1.csv");

			
				JavaRDD<Route> delayRdd = 
						rawDataDelay.javaRDD().map(row -> {
							Route r = new Route();
							//	r.setAirLineCode(row.getString(1));
//								r.setAirlineId(String)
								//r.setSrcId(String)
								r.setSrc(row.getString(14));
								//r.setDstId(String)
								r.setDst(row.getString(23));
								r.setDelay( Long.parseLong(row.getString(30)) );
//								r.setSrc(String)
//								r.setDst(String)
//								r.setRelationType(String)
								return r;
						});
			
				Dataset<Row> delayDS = session.createDataFrame(delayRdd.rdd(), Route.class);
							 delayDS.createOrReplaceTempView("DELAY");
				delayDS.show();			
		GraphFrame gf = new GraphFrame(airports, routes);
		
			gf.vertices().show();

			//1. Check graphframe is good setup
			gf.vertices().filter("country = 'US'").show();
			
			//2. Number of airports in india
			//System.out.println("Airports in USA ----> " + gf.vertices().filter("country = 'United States'").count());
			
			
			gf.degrees().createOrReplaceTempView("degrees");
			
			session.sql("select a.airportName, a.State, a.Country, d.degree from airports a, degrees d where a.airportIataCode = d.id order by d.degree desc").show(10);
			
			
//			//3. Number of flights leaving EWR
//			gf.outDegrees().filter("id = 'EWR'").show();
//			
//			System.out.println("==========> " + gf.edges().filter("src = 'EWR'").count());
////			
////			
// 
//		// Top 10 flights from airport to airport
//			
			gf.triplets().filter("src.country = 'United States' and dst.country = 'India'").createOrReplaceTempView("US_TO_INDIA");
//			
			//session.sql("select u.src.state source_city, u.dst.state destination_city,a.airlineName from US_TO_INDIA u, airlines a where u.edge.airLineCode = a.airlineId").show(50);
			// session.sql("select u.src.state source_city, u.dst.state destination_city,u.edge.airlineCode from US_TO_INDIA u ").show();
			
			session.sql("select u.src.state source_city, u.dst.state destination_city,a.airlineName,d.delay "
					+ "from US_TO_INDIA u, airlines a,DELAY d where u.edge.airLineCode = a.airlineId and d.src = u.src.src and d.dst = u.dst.dst").show(50);
			
//		//	gf.triangleCount().run().filter("id = 'SFO'").show(100);
//			
//			 gf.bfs().fromExpr("id = 'SFO'").toExpr("id = 'BUF'").maxPathLength(2).run().show();
//			
////			bfsResult.foreach(row -> {
////				int sz = row.size();
////				for(int x = 0 ; x < sz ; x++) {
////					System.out.println("----------> " + row.get(x).toString());
////				}
////				System.out.println("###############################");;
////			});
////		////			//gf.in
//			// 4.  pagerank
//			//gf.pageRank().resetProbability(0.2).maxIter(5).run().vertices().show();
//			
//			
//			//4. Airport with maximum inbound flights
			
	}
}
