package big_data_analytics_java.chp2;

public class AprioriUtil {

	public static double findSupport(Integer numOfTrans, Integer totalTrans) {
		double results = numOfTrans.doubleValue() / totalTrans.doubleValue();
		return results;
	}
	
	public static double findConfidence(Integer numOfTransWithBothLhsAndRhs, Integer numOfTransWithLhs) {
		double results = (numOfTransWithBothLhsAndRhs.doubleValue() / numOfTransWithLhs.doubleValue()) * 100;
		return results;
	}
	
	public static double findConfidenceD(Integer numOfTransWithLhs, Integer numOfTransWithRhs) {
		double results = (numOfTransWithLhs.doubleValue() / numOfTransWithRhs.doubleValue()) * 100;
		return results;
	}
}
