package big_data_analytics_java.chp5;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousingDataExplore {

	public static void main(String[] args) {
		SparkConf sc = new SparkConf().setMaster("local[*]");
	    SparkSession spark = SparkSession
	    	      				.builder()
	    	      				.config(sc)
	    	      				.appName("JavaBinarizerExample")
	    	      				.getOrCreate();
	    Dataset<Row> data = spark.read().csv("data/kc_house_data.csv");
	    	System.out.println("Number of Rows --> " + data.count());
	    	
	    		data.createOrReplaceTempView("houses");
	    Dataset<Row> avgPrice = spark.sql("select _c16 zipcode,avg(_c2) avgPrice from houses group by _c16 order by avgPrice desc");
	    	avgPrice.show();
	    	
	}
	
	
}
