package big_data_analytics_java.chp7;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PTreeExploration {
	public static void main(String[] args) {
		SparkConf c = new SparkConf().setMaster("local");
	    
		SparkSession spark = SparkSession
	      .builder()
	      .config(c)
	      .appName("LoanLendingTree")
	      .getOrCreate();
	    
		
	    Dataset<Row> rowDS = spark.read().csv("data/loan/loan_train.csv");
	    
	    	rowDS.describe("_c0","_c1","_c2","_c3","_c4","_c5","_c6","_c7","_c8","_c9","_c10","_c11","_c12").show();
//	    		rowDS.show();
//	    		
//	    			 rowDS.createOrReplaceTempView("loans");
//	    
//	    // 1. total rows in the dataset
//	    System.out.println("Number of rows --> " + rowDS.count());
//	     	
//	    	rowDS.printSchema();
//	    	rowDS.show();
////	    
////	    //Number of males and females
//	    Dataset<Row> maleFemaleDS = spark.sql("select _c1 gender,count(*) cnt from loans group by _c1");
//	    	maleFemaleDS.show();
//	    	
//		Dataset<Row> chistoryDS = spark.sql("select _c10 creditHistory,count(*) cnt from loans group by _c10");
//		chistoryDS.show();	    	
//	    	
//		Dataset<Row> loanAmtDS = spark.sql("select * from loans where _c8 = '' or _c8 is null");
//		loanAmtDS.show();	    	
//		
//		Dataset<Row> loanStatusDS = spark.sql("select count(*) from loans where _c12 = '' or _c12 is null");
//		loanStatusDS.show();	    		
////		Dataset<Row> maleFemaleDS = spark.sql("select _c1 gender,count(*) cnt from loans group by _c1");
////	    	maleFemaleDS.show();	    	
	}
}
