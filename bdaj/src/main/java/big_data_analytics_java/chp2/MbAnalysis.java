package big_data_analytics_java.chp2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class MbAnalysis {

	private static String appName = "MbAnalysis";
	private static String master = "local[*]";
	private static String FILE_NAME = "resources/data/retail/retail_small.txt";
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> rddX = sc.textFile(FILE_NAME);	
			//System.out.println(rddX.count());
		UniqueCombinations uc = new UniqueCombinations();
		JavaRDD<Map<String,String>> combStrArr = rddX.map(s -> uc.findCombinations(s));
		JavaRDD<Set<String>> combStrKeySet = combStrArr.map(m -> m.keySet());
		JavaRDD<String> combStrFlatMap = combStrKeySet.flatMap((Set<String> f) -> f.iterator());
		JavaPairRDD<String, Integer> combCountIndv = combStrFlatMap.mapToPair(s -> new Tuple2(s, 1));
		JavaPairRDD<String, Integer> combCountTotal = combCountIndv.reduceByKey((Integer x, Integer y) -> x.intValue() + y.intValue());
		
		List<Tuple2<String,Integer>> combCountIndvColl = combCountTotal.collect();
		for (Tuple2<String, Integer> tuple2 : combCountIndvColl) {
			System.out.println(tuple2._1 + "," + tuple2._2);
		}
//		List<String> results = combStrFlatMap.collect();
//		for (String rs : results) {
//			System.out.println(rs);
//		}
//		List<Map<String,String>> cl = combStrArr.collect();
//		for (Map<String,String> list : cl) {
//			for (String key : list.keySet()) {
//				System.out.println("Key --> " + key);
//			}	
//		}

	}

}
