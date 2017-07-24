package big_data_analytics_java.chp2;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.api.java.function.Function;

public class FPGrowthSample {
	private static String appName = "FpGrowth_Example";
	private static String master = "local[*]";
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		JavaSparkContext sc = new JavaSparkContext(conf);		
	JavaRDD<String> data = sc.textFile("resources/data/retail/retail_small_fpgrowth.txt");

	JavaRDD<List<String>> transactions = data.map(
	  new Function<String, List<String>>() {
	    public List<String> call(String line) {
	      String[] parts = line.split(" ");
	      return Arrays.asList(parts);
	    }
	  }
	);

	FPGrowth fpg = new FPGrowth().setMinSupport(0.4).setNumPartitions(1);
	FPGrowthModel<String> model = fpg.run(transactions);

	for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
	  System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
	}

	double minConfidence = 0.0;
	for (AssociationRules.Rule<String> rule
	  : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
	  System.out.println(
	    rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
	}
	}
}
