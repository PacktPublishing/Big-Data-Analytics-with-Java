package big_data_analytics_java.chp8;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Function1;

public class DefaultCols {

	public static void main(String[] args) {
		SparkConf c = new SparkConf().setMaster("local[*]");
	    
		SparkSession spark = SparkSession
	      .builder()
	      .config(c)
	      .appName("LoanLendingTree")
	      .getOrCreate();
	    

	    Dataset<Row> defaultData = spark.read().csv("C:/harpreet/datasets/LoanStats3a.csv");
	    			 defaultData.show();
	    			 
	    			 defaultData.createOrReplaceTempView("loans");
	    			 
	    JavaRDD<Row> rdd = defaultData.toJavaRDD();
	    JavaRDD<LoanVO> data = rdd.map( r -> {
	    	if(r.size() < 100) return null;
	    	LoanVO lvo = new LoanVO();
	    	String loanAmt = r.getString(2).replaceAll("\"", "").trim();
	    	String loanId = r.getString(0).replaceAll("\"", "").trim();
	    	System.out.println("loanId --> " + loanId);
	    	String fundedAmt = r.getString(3).replaceAll("\"", "").trim();
	    	String grade = r.getString(8).replaceAll("\"", "").trim();
	    	String subGrade = r.getString(9).replaceAll("\"", "").trim();
	    	String empLength = r.getString(11).replaceAll("\"", "").trim();
	    	String homeOwn = r.getString(12).replaceAll("\"", "").trim();
	    	String annualInc = r.getString(13).replaceAll("\"", "").trim();
	    	String loanStatus = r.getString(16).replaceAll("\"", "").trim();
	    	
	    	if(loanAmt.contains("N/A") || loanId.contains("N/A") || fundedAmt.contains("N/A") || grade.contains("N/A") ||
	    			subGrade.contains("N/A") || empLength.contains("N/A") || homeOwn.contains("N/A") || annualInc.contains("N/A") || loanStatus.contains("N/A")) 
	    		return null;
	    	
	    	if(null == annualInc || "".equals(annualInc)) return null;
	    		   lvo.setLoanAmt(Double.parseDouble(loanAmt));
	    		   lvo.setLoanId(Integer.parseInt(loanId));
	    		   lvo.setFundedAmt(Double.parseDouble(fundedAmt));
//	    		   lvo.setFundedAmtInv(Double.parseDouble(darr[4].replaceAll("\"", "").trim()));
	    		   lvo.setGrade(grade);
	    		   lvo.setSubGrade(subGrade);
	    		   lvo.setEmpLengthStr(empLength);
	    		   lvo.setHomeOwnership(homeOwn);
	    		   lvo.setAnnualInc(Double.parseDouble(annualInc));
	    		   
	    		   lvo.setLoanStatusStr(loanStatus);
	    		   
	    		   if(loanStatus.contains("Fully")) lvo.setLoanStatus(1.0);
	    		   else lvo.setLoanStatus(0.0);
	    		   
//	    		   lvo.setLoanDesc(darr[20].replaceAll("\"", "").trim());
//	    		   lvo.setTitle(darr[22].replaceAll("\"", "").trim());
//	    		   lvo.setZipCode(darr[23].replaceAll("\"", "").trim());
	    		   
	    		   return lvo;

	    } ).filter(f ->  {
	    	if(f == null) return false;
	    	else return true;
	    });

	    System.out.println("cnt --> " + data.first());
//	    	spark.sql("select distinct _c12 from loans").show(100);
	}

}
