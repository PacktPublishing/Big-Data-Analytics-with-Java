package big_data_analytics_java.chp2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;

import io.netty.util.internal.StringUtil;

public class UniqueCombinations implements Serializable{


	
	public UniqueCombinations() {
		
	}
	
	public static Map<String,String> findCombinations(String row) {
		  List<String> fullSet = new ArrayList<>();
		  Map<String,String> globalKeys = new LinkedHashMap<>();
		//String row = "a,b,c";
		List<String> previousPattern = fetchSingleSet(globalKeys,row); 
			fullSet.addAll(previousPattern);

		int loopRunLength = previousPattern.size() - 2;
		for(int i = 0; i < loopRunLength ; i++) {
			previousPattern = fetchMorePatternsFromPrevPatterns(fullSet,globalKeys,previousPattern);
		}
		

		
		return globalKeys;
	}
	
	private static List<String> fetchMorePatternsFromPrevPatterns(List<String> fullSet,Map<String,String> globalKeys,List<String> previousPattern) {
		
		Map<String,String> keys = new LinkedHashMap<>();

		for(int i = 0 ; i < previousPattern.size() ; i++) {
			for(int j = i+1 ; j < previousPattern.size() ; j++) {
	            Set<String> combinationList = new HashSet<>();
	            	combinationList.addAll(Arrays.asList(previousPattern.get(i).split(",")));
	            	combinationList.addAll(Arrays.asList(previousPattern.get(j).split(",")));
	            String[] combinationArr = combinationList.toArray(new String [combinationList.size()]);
	            Arrays.sort(combinationArr);
	            String newKey = "";
	            for (String strVal : combinationArr) {
	            	newKey = ("".equals(newKey)) ? strVal : newKey + "," + strVal;
				}
	            keys.put(newKey, "PRESENT");
			}
		}
		globalKeys.putAll(keys);
		Set<String> keySet = keys.keySet();
		for (String k : keySet) {
			fullSet.add(k);
		}
		
		return new ArrayList<>(keySet);
		
	}

	public static List<String> fetchSingleSet(Map<String,String> globalKeys,String pattern) {
		List<String> result = new ArrayList<>();
		String[] arr = pattern.split(",");
        for(int i=0; i<arr.length; i++)
        {
        	globalKeys.put(arr[i], "PRESENT");
        	result.add(arr[i]);
        }
        return result;
	}
	
	
	public List<Rule> getRules(String selStr) {
		 List<String> sl = Arrays.asList(selStr.split(","));
		 List<Rule> rules = new ArrayList<>();
		 for(int i = 0 ; i < sl.size() ; i++ ) {
			 Rule r = new Rule();
			 	r.setRhs(sl.get(i));
			 	r.setLhs(getLhs(sl, i));
			 	rules.add(r);
		 }
		 return rules;
	}
	
	public String getLhs(List<String> sl, int leaveIndex) {
		List<String> clone = new ArrayList<>(sl);
			clone.remove(leaveIndex);
			return StringUtils.join(clone, ",");
	}
	
	public static void main(String[] args) {
		UniqueCombinations uc = new UniqueCombinations();
			
			for (String key : uc.findCombinations("milk,chocolate cookies,apples,diapers").keySet()) {
				System.out.println("Key --> " + key);
			}			
	}

}
