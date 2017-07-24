package big_data_analytics_java.chp2;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class Apriori {

	private static String appName = "Apriori_Example";
	private static String master = "local";
	private static String FILE_NAME = "resources/data/retail/retail_small.txt";
	private static final double MIN_SUPPORT = 0.5;
	private static final double MIN_CONFIDENCE = 0.8; // or 80%
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> rddX = sc.textFile(FILE_NAME);
		AprioriUtil au = new AprioriUtil();
		Long transactionCount = rddX.count();
		Broadcast<Integer> broadcastVar = sc.broadcast(transactionCount.intValue());

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
		
		
		Map<String,Integer> freqMap = combCountTotal.collectAsMap();
		Broadcast<Map<String,Integer>> bcFreqMap = sc.broadcast(freqMap);
		
		System.out.println("Total combinations count initiali" + combCountTotal.count());
		JavaPairRDD<String,Integer> combFilterBySupport = combCountTotal.filter(c -> c._2.intValue() >= 2);
		//System.out.println("Total combFilterBySupport count initiali" + combFilterBySupport.count());
		
		JavaPairRDD<String,Integer> freqBoughtTogether = combFilterBySupport.filter(s -> s._1.indexOf(",") > 0);
		
		List<Tuple2<String,Integer>> combCountIndvColl7 = freqBoughtTogether.collect();
		for (Tuple2<String, Integer> tuple2 : combCountIndvColl7) {
			System.out.println("--------------->" + tuple2._1 + "," + tuple2._2);
		}
		
		JavaRDD<Rule> assocRules = freqBoughtTogether.flatMap(tp -> {
			List<Rule> rules = uc.getRules(tp._1);
			for (Rule rule : rules) {
				String lhs = rule.getLhs();
				String rhs = rule.getRhs();
				Integer lhsCnt = bcFreqMap.value().get(lhs);
				Integer rhsCnt = bcFreqMap.value().get(rhs);
				Integer lhsRhsBothCnt = bcFreqMap.value().get(tp._1);
				double supportLhs = au.findSupport(lhsCnt, broadcastVar.value());
				double supportRhs = au.findSupport(rhsCnt, broadcastVar.value());
				double confidence = au.findConfidence(lhsRhsBothCnt, lhsCnt);
					rule.setSupportLhs(supportLhs);
					rule.setSupportRhs(supportRhs);
					rule.setConfidence(confidence);
			}
			return rules.iterator();
		});
		
		List<Rule> rulesColl = assocRules.collect();
		for (Rule rl : rulesColl) {
			System.out.println(rl.getLhs() + " => " + rl.getRhs() + " , " + rl.getConfidence());
		}
	}

}
