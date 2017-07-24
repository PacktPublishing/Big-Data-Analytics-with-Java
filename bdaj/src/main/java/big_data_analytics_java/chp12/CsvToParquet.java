package big_data_analytics_java.chp12;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class CsvToParquet {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]");
		SparkSession session = SparkSession
								.builder()
								.config(conf)
							    .appName("FA")
							    .getOrCreate();
	
		//Airlines
		Dataset<Row> rawDataAirline = session.read().csv("data/flight/On_Time_On_Time_Performance_2017_1.csv");
				     rawDataAirline.createOrReplaceTempView("all_data");
		Dataset<Row> filteredData = session.sql("select _c7 airline_id, _c14 origin, _c16 origin_state, _c31 dep_delay, "
									+ "_c23 dest, _c25 dest_state, _c42 arr_delay,_c54 distance, _c57 weather_delay, _c47 cancelled from all_data");
			filteredData.show();
			
			filteredData.createOrReplaceTempView("flights");
			
		session.sql("select count(*) from (select distinct origin,dest from flights)").show();;
			
		session.sql("select * from flights where dep_delay > 30 and origin = 'SFO'").show();
//		
//		
		session.sql("select origin_state, count(*) cnt from flights where cancelled > 0 group by origin_state order by cnt desc").show();;
//		
//		session.sql("select distinct dest,dest_state from flights where origin = 'SFO'").show();;
			
			//filteredData.write().parquet("flight_data.parquet");
		
		

	}
	
}
