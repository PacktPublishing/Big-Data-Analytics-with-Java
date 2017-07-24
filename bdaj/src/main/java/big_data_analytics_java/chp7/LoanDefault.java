package big_data_analytics_java.chp7;

//import java.io.Serializable;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.mllib.linalg.Vectors;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Encoder;
//import org.apache.spark.sql.Encoders;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.RowFactory;
//import org.apache.spark.sql.SparkSession;

public class LoanDefault {

//	public static void main(String[] args) {
//		SparkConf c = new SparkConf().setMaster("local[*]");
//	    SparkSession spark = SparkSession
//	      .builder()
//	      .config(c)
//	      .appName("JavaDecisionTreeClassificationExample")
//	      .getOrCreate();
//
//	    //1. load the full data
//	    Dataset<String> loanData = spark.read().textFile("data/loan/loan_train.csv");
//
//	    //filter Rows of label and feature vectors
//	    Dataset<LoanVO> loanDataMapped = loanData.map(dataRow -> {
//			String[] rowArr = dataRow.split(",");
//			String[] sArr = new String[4];
//				sArr[0] = (null != rowArr[10] && !"".equals(rowArr[10].trim())) ? rowArr[10] : "1";
//				sArr[1] = (null != rowArr[9] && !"".equals(rowArr[9].trim())) ? rowArr[9] : "360";  ;
//				sArr[2] = (null != rowArr[8] && !"".equals(rowArr[8].trim())) ? rowArr[8] : "0";  ;
//				sArr[3] = rowArr[12];
//				LoanVO lvo = new LoanVO();
//					lvo.setFeature1(sArr[0]);
//					lvo.setFeature2(sArr[1]);
//					lvo.setFeature3(sArr[2]);
//					lvo.setResult(sArr[3]);
//					return lvo;
//	    }, Encoders.bean(LoanVO.class));
//
//	    loanDataMapped.show();
//
//
//	    Dataset<Row> loanVectors =
//	    		loanDataMapped.map(loanVO -> {
//	    		   return RowFactory.create(
//	    				   loanVO.getResultDbl(),Vectors.dense(new double[]{loanVO.getFeature1Dbl(),loanVO.getFeature2Dbl(),loanVO.getFeature3Dbl()}));
//	    		});
//	    loanVectors.show();
//	}
//
//	public static class LoanVO implements Serializable {
//		String feature1;
//		String feature2;
//		String feature3;
//		String result;
//		public String getFeature1() {
//			return feature1;
//		}
//		public void setFeature1(String feature1) {
//			this.feature1 = feature1;
//		}
//		public String getFeature2() {
//			return feature2;
//		}
//		public void setFeature2(String feature2) {
//			this.feature2 = feature2;
//		}
//		public String getFeature3() {
//			return feature3;
//		}
//		public void setFeature3(String feature3) {
//			this.feature3 = feature3;
//		}
//		public String getResult() {
//			return result;
//		}
//		public void setResult(String result) {
//			this.result = result;
//		}
//
//		public double getResultDbl() {
//			return Double.parseDouble(this.result);
//		}
//		public double getFeature1Dbl() {
//			return Double.parseDouble(this.feature1);
//		}
//		public double getFeature2Dbl() {
//			return Double.parseDouble(this.feature2);
//		}
//		public double getFeature3Dbl() {
//			return Double.parseDouble(this.feature3);
//		}
//
//	}
//

}
