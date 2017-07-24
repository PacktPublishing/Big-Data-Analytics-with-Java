package big_data_analytics_java.chp8;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Function1;

public class DataExploreLendingTree {

	public static void main(String[] args) {
		SparkConf c = new SparkConf().setMaster("local");
	    
		SparkSession spark = SparkSession
	      .builder()
	      .config(c)
	      .appName("LoanLendingTree")
	      .getOrCreate();
	    

	    
//	    System.out.println("Number of rows -->" + dataRdd.count());
	    
	    //1. For our Analysis we will filter all teh 'current' loans

	    
    Dataset<Row> defaultData = spark.read().csv("C:/harpreet/datasets/LoanStats3a.csv");
		 		 // defaultData.show();
		 
	JavaRDD<Row> rdd = defaultData.toJavaRDD();
	
	JavaRDD<LoanVO> data = rdd.map( r -> {
			if(r.size() < 100) return null;
			LoanVO lvo = new LoanVO();
			String loanId = r.getString(0).trim();
//			System.out.println("loanId --> " + loanId);
			String loanAmt = r.getString(2).trim();
//			System.out.println("loanId --> " + loanId);;
//			System.out.println("localid --> " + loanAmt.trim());;
//			System.out.println("loanId --> " + loanId);
			String fundedAmt = r.get(3).toString().trim();
			String grade = r.get(8).toString().trim();
			String subGrade = r.get(9).toString().trim();
			String empLength = r.get(11).toString().trim();
			String homeOwn = r.get(12).toString().trim();
			String annualInc = r.getString(13);
			String loanStatus = r.get(16).toString().trim();
						
			if(null == annualInc || "".equals(annualInc) || 
					null == loanAmt || "".equals(loanAmt) || 
					null == grade || "".equals(grade) || 
					null == subGrade || "".equals(subGrade) || 
					null == empLength || "".equals(empLength) || 
					null == homeOwn || "".equals(homeOwn) || 
					null == loanStatus || "".equals(loanStatus)) return null;
			
			
			if(loanAmt.contains("N/A") || loanId.contains("N/A") || fundedAmt.contains("N/A") || grade.contains("N/A") ||
					subGrade.contains("N/A") || empLength.contains("N/A") || homeOwn.contains("N/A") || annualInc.contains("N/A") || loanStatus.contains("N/A")) 
				return null;
			
			
			
			if("Current".equalsIgnoreCase(loanStatus)) return null;
			
				   lvo.setLoanAmt(Double.parseDouble(loanAmt));
				   lvo.setLoanId(Integer.parseInt(loanId));
				   lvo.setFundedAmt(Double.parseDouble(fundedAmt));
			//	   lvo.setFundedAmtInv(Double.parseDouble(darr[4].trim()));
				   lvo.setGrade(grade);
				   lvo.setSubGrade(subGrade);
				   lvo.setEmpLengthStr(empLength);
				   lvo.setHomeOwnership(homeOwn);
				   lvo.setAnnualInc(Double.parseDouble(annualInc.trim()));
				   
				   lvo.setLoanStatusStr(loanStatus);
				   
				   if(loanStatus.contains("Fully")) lvo.setLoanStatus(1.0);
				   else lvo.setLoanStatus(0.0);
				   
			//	   lvo.setLoanDesc(darr[20].trim());
			//	   lvo.setTitle(darr[22].trim());
			//	   lvo.setZipCode(darr[23].trim());
				   
				   return lvo;
			
			} ).filter(f ->  {
			if(f == null) return false;
			else return true;
			});
	    	
	    Dataset<Row> dataDS = spark.createDataFrame(data.rdd(), LoanVO.class);
	    			 dataDS.createOrReplaceTempView("loans");
	    			 
	    			 dataDS.describe("annualInc").show();
	    			 dataDS.describe("loanAmt").show();
	    			 dataDS.describe("fundedAmt").show();
	    		System.out.println("end");;	 
	    		//dataDS.show();
	    
//	    //2. Counting loans by grades
//	    Dataset<Row> loansByGrade = spark.sql("select count (distinct grade) from loans");
//	    			 loansByGrade.show();
//	    			 
//	 //   Dataset<Row> grpByGrade = spark.sql("select grade,count(*) from 
//	    			    Dataset<Row> loansBySubGrade = spark.sql("select count (distinct subGrade) from loans");
//		    			 loansBySubGrade.show();
//		    			 
//	    Dataset<Row> loansByHomeOwnership = spark.sql("select count (distinct homeOwnership) from loans");
//	    			 loansByHomeOwnership.show();		    			 
	}

}
