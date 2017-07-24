package big_data_analytics_java.chp7;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PTreeDataCleaner {

	
	public static void main(String[] args) {
		SparkConf c = new SparkConf().setMaster("local");
	    SparkSession spark = SparkSession
	      .builder()
	      .config(c)
	      .appName("LoanLendingTree")
	      .getOrCreate();
	    Dataset<Row> rowDS = spark.read().csv("data/loan/loan_train.csv");
	    			 rowDS.createOrReplaceTempView("loans");
	    Dataset<Row> rowAvgDS = spark.sql("select _c10,count(*) from loans where _c10 is not null group by _c10 ");
//	    Row avgRow = rowAvgDS.collectAsList().get(0);
//	    double avgCreditHistory = (avgRow.getDouble(0) > 0.5)? 1.0:0.0;
	    
	    	rowAvgDS.show();
//	    	System.out.println(" avg credit history value --> " + avgCreditHistory);
	    
		    Dataset<Row> avgLoanAmtRow = spark.sql("select avg(_c8) avgLoanAmount from loans where _c8 is not null");
		     avgLoanAmtRow.show();
	    	
	}
	
	
}