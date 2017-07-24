package big_data_analytics_java.chp7;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

public class TreeGermanLoanDefault {
	public static void main(String[] args) {
		SparkConf c = new SparkConf().setMaster("local");
	    SparkSession spark = SparkSession
	      .builder()
	      .config(c)
	      .appName("LoanLendingTree")
	      .getOrCreate();
	 
	JavaRDD<String> dataDS = spark.sparkContext().textFile("data/german/german.data-numeric", 1).toJavaRDD();
	
	JavaRDD<Row> rowDS1 = dataDS.map(row -> {
		String[] s = row.split("  ");
		return RowFactory.create(Double.parseDouble(s[1].trim()),Double.parseDouble(s[s.length - 1].trim()));
	});
		
//	rowDS.foreach(f -> System.out.println(f.getDouble(1)));
		
	
	  //  Dataset<Row> rowDS = spark.read().text("data/german/german.data-numeric").map
	    	//		 rowDS.createOrReplaceTempView("loans");
	    			 
	    	//		 rowDS.show();
	Dataset<Row> rowDS = spark.createDataFrame(rowDS1.rdd(), Row.class);
//	    System.out.println("Number of rows --> " + rowDS.count());
//	     	
	    	rowDS.printSchema();
//	    
//	    //Number of males and females
//	    Dataset<Row> maleFemaleDS = spark.sql("select _c1 gender,count(*) cnt from loans group by _c1");
//	    	maleFemaleDS.show();
//	    	
//		Dataset<Row> chistoryDS = spark.sql("select _c10 creditHistory,count(*) cnt from loans group by _c10");
//		chistoryDS.show();	    	
//	    	
//		Dataset<Row> loanAmtDS = spark.sql("select * from loans where _c8 = '' or _c8 is null");
//		loanAmtDS.show();	    	
//		
//		Dataset<Row> loanStatusDS = spark.sql("select * from loans where _c12 = '' or _c12 is null");
//		loanStatusDS.show();	    		
//		Dataset<Row> maleFemaleDS = spark.sql("select _c1 gender,count(*) cnt from loans group by _c1");
//	    	maleFemaleDS.show();	    	
	}
}
