package big_data_analytics_java.chp11.flight_analytics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Descending;
import org.graphframes.GraphFrame;
import org.graphframes.lib.PageRank;
//import org.graphstream.graph.Graph;
//import org.graphstream.graph.implementations.DefaultGraph;
//import org.graphstream.graph.implementations.MultiGraph;
//import org.graphstream.graph.implementations.SingleGraph;

import scala.collection.mutable.HashMap;

import static org.apache.spark.sql.functions.*;

import java.util.List;
import java.util.Map;

public class FlightsGraphAnalysis {

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
		//Dataset<Row> rawDataRoute = session.read().csv("data/flight/routes.dat");
			Dataset<Row> rawDataRoute = session.read().csv("data/flight/routes.dat");

		
			JavaRDD<Route> routesRdd = 
					rawDataRoute.javaRDD().map(row -> {
						Route r = new Route();
							r.setAirLineCode(row.getString(1));
//							r.setAirlineId(String)
							
							r.setSrc(row.getString(2));
							r.setSrcId(row.getString(3));
							//r.setDstId(String)
							r.setDst(row.getString(4));
							r.setDstId(row.getString(5));
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
		
//			gf.vertices().show();
//
//			//1. Check graphframe is good setup
//			gf.vertices().filter("country = 'US'").show();
			
			//2. Number of airports in india
			//System.out.println("Airports in USA ----> " + gf.vertices().filter("country = 'United States'").count());
			
			
//			gf.degrees().createOrReplaceTempView("degrees");
//			
//			session.sql("select a.airportName, a.State, a.Country, d.degree from airports a, degrees d where a.airportIataCode = d.id order by d.degree desc").show(10);
//			
//			
////			//3. Number of flights leaving EWR
////			gf.outDegrees().filter("id = 'EWR'").show();
////			
////			System.out.println("==========> " + gf.edges().filter("src = 'EWR'").count());
//////			
//////			
//// 
////		// Top 10 flights from airport to airport
////			
//			gf.triplets().filter("src.country = 'United States' and dst.country = 'India'").createOrReplaceTempView("US_TO_INDIA");
////			
//			//session.sql("select u.src.state source_city, u.dst.state destination_city,a.airlineName from US_TO_INDIA u, airlines a where u.edge.airLineCode = a.airlineId").show(50);
//			// session.sql("select u.src.state source_city, u.dst.state destination_city,u.edge.airlineCode from US_TO_INDIA u ").show();
//			
////			session.sql("select u.src.state source_city, u.dst.state destination_city,a.airlineName,d.delay "
////					+ "from US_TO_INDIA u, airlines a,DELAY d where u.edge.airLineCode = a.airlineId and d.src = u.src.src and d.dst = u.dst.dst").show(50);
//			
//			
//			//BFS test
//			gf.triplets().filter("src.airportIataCode='SFO' and dst.airportIataCode='BUF'").show();
//			
//			System.out.println("end the game");;
			
			
			
//		//	gf.triangleCount().run().filter("id = 'SFO'").show(100);
//			
//			 Dataset<Row> sfoToBufDS = gf.bfs().fromExpr("id = 'SFO'").toExpr("id = 'BUF'").maxPathLength(2).run();
//			 			  sfoToBufDS.createOrReplaceTempView("sfo_to_buf");
//			 			  
//			 			session.sql("select distinct from.state , v1.state, to.state from sfo_to_buf").show(100);
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
		Dataset pg = gf.pageRank().resetProbability(0.15).maxIter(5).run().vertices();
				pg.createOrReplaceTempView("pageranks");
//				
			session.sql("select * from pageranks order by pagerank desc").show(20);
			
//			
//			//4. Airport with maximum inbound flights
//		Dataset<Row> ukVertices = gf.vertices().filter("country = 'United States'");
//					 ukVertices.createOrReplaceTempView("ukAirports");
//					 ukVertices.show();
//					 gf.edges().createOrReplaceTempView("ALL_EDGES");
//		
//		Dataset<Row> ukEdges = session.sql("select * from ALL_EDGES where srcId in (select airportId from ukAirports) and dstId in (select airportId from ukAirports)");
//		Dataset<Row> filtered = session.sql("select * from ukAirports where airportId in ((select srcId from ukRoutes) union (select dstId from ukRoutes))");
//		GraphFrame ukSubGraph = new GraphFrame(filtered, ukEdges);
//		Graph graph = new SingleGraph("SimpleGraph");
//		List<Row> ukv = ukSubGraph.vertices().collectAsList();
//		for (Row row : ukv) {
//			graph.addNode(row.getString(2)).setAttribute("ui.label", row.getString(8));
//		}
//		List<Row> uke = ukSubGraph.edges().distinct().collectAsList();
//		Map<String, String> keys = new java.util.HashMap<>();
//		for (Row row : uke) {
//		   if(!keys.containsKey(row.getString(9) + row.getString(5))) {
//			   graph.addEdge(row.getString(9) + row.getString(5), row.getString(9), row.getString(5));
//			   keys.put(row.getString(9) + row.getString(5), "KEY");
//		   }
//		}		
//		
//
//		graph.display();
					 
//		Dataset<Row> franceEdges = gf.edges().filter("country = 'France'");
//		franceEdges.show();
//		
//		GraphFrame franceGF = new GraphFrame(franceVertices,  gf.edges());
//			franceGF.vertices().show();
//			franceGF.edges().show();
	}
}
