package big_data_analytics_java.chp5;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HeartDataExplore {
	public static void main(String[] args) {
		SparkConf sc = new SparkConf().setMaster("local[*]");
	    SparkSession spark = SparkSession
	    	      				.builder()
	    	      				.config(sc)
	    	      				.appName("JavaBinarizerExample")
	    	      				.getOrCreate();
	    Dataset<Row> data = spark.read().csv("data/heart_disease_data.csv");
	    	System.out.println("Number of Rows --> " + data.count());
	    	
	    	data.createOrReplaceTempView("heartdiseasedata");
	 	 
	    Dataset<Row> menWomenCnt = spark.sql("select _c1 sex,count(*) from heartdiseasedata group by _c1");
	    	menWomenCnt.show();

	    Dataset<Row> menWomenAvgAge = spark.sql("select _c1 sex,avg(_c0) from heartdiseasedata group by _c1");
	    	menWomenAvgAge.show();
	    		
	    Dataset<Row> menWomenMinAge = spark.sql("select _c1 sex,min(_c0) from heartdiseasedata group by _c1");
	    	menWomenMinAge.show();	    		
	    		
	}
}
