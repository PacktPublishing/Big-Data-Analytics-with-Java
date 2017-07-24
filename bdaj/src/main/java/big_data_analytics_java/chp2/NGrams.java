package big_data_analytics_java.chp2;

import java.util.LinkedHashMap;
import java.util.List;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class NGrams implements Serializable{

	private static List<String[]> itemsets = new ArrayList() ;
	private static int numItems;

	
	public static void main(String[] args) {
		String test = "a,b,c,d";
		
		String[] arr = test.split(",");
		
		int n = test.length();
		List<String[]> ngramList = new ArrayList();
		createItemsetsOfSize1();
		ngramList.add(itemsets.get(0));
		while (itemsets.size()>0)
        {
			createNewItemsetsFromPreviousOnes();
			if(itemsets.size() >0)
			ngramList.add(itemsets.get(0));
        }
		
		
//		for(String[] sarr:itemsets) {
//			for (String s : sarr) {
//				System.out.println(s + ",");
//			}
//		}
		

		for(String[] sarr:ngramList) {
			for (String s : sarr) {
				System.out.println(s + ",");
			}
		}
	}
	
	public List<String[]> getCombinations(String test) {
		String[] arr = test.split(",");
		
		int n = test.length();
		List<String[]> ngramList = new ArrayList();
		createItemsetsOfSize1();
		ngramList.add(itemsets.get(0));
		while (itemsets.size()>0)
        {
			createNewItemsetsFromPreviousOnes();
			if(itemsets.size() >0)
			ngramList.add(itemsets.get(0));
        }
		return ngramList;
	}
	
	
	
	   private static void createNewItemsetsFromPreviousOnes()
	    {
	    	// by construction, all existing itemsets have the same size
	    	int currentSizeOfItemsets = itemsets.get(0).length;
	    	System.out.println("Creating itemsets of size "+(currentSizeOfItemsets+1)+" based on "+itemsets.size()+" itemsets of size "+currentSizeOfItemsets);
	    		
	    	HashMap<String, String[]> tempCandidates = new HashMap<String, String[]>(); //temporary candidates
	    	
	        // compare each pair of itemsets of size n-1
	        for(int i=0; i<itemsets.size(); i++)
	        {
	            for(int j=i+1; j<itemsets.size(); j++)
	            {
	                String[] X = itemsets.get(i);
	                String[] Y = itemsets.get(j);

	                
	                //make a string of the first n-2 tokens of the strings
	                String [] newCand = new String[currentSizeOfItemsets+1];
	                for(int s=0; s<newCand.length-1; s++) {
	                	newCand[s] = X[s];
	                }
	                    
	                int ndifferent = 0;
	                // then we find the missing value
	                for(int s1=0; s1<Y.length; s1++)
	                {
	                	boolean found = false;
	                	// is Y[s1] in X?
	                    for(int s2=0; s2<X.length; s2++) {
	                    	if (X[s2]==Y[s1]) { 
	                    		found = true;
	                    		break;
	                    	}
	                	}
	                	if (!found){ // Y[s1] is not in X
	                		ndifferent++;
	                		// we put the missing value at the end of newCand
	                		newCand[newCand.length -1] = Y[s1];
	                	}
	            	
	            	}
	                
	                // we have to find at least 1 different, otherwise it means that we have two times the same set in the existing candidates
	                assert(ndifferent>0);
	                
	                
	                if (ndifferent==1) {
	                    // HashMap does not have the correct "equals" for int[] :-(
	                    // I have to create the hash myself using a String :-(
	                	// I use Arrays.toString to reuse equals and hashcode of String
	                	Arrays.sort(newCand);
	                	tempCandidates.put(Arrays.toString(newCand),newCand);
	                }
	            }
	        }
	        
	        //set the new itemsets
	        itemsets = new ArrayList<String[]>(tempCandidates.values());
	        	    	System.out.println("Created "+itemsets.size()+" unique itemsets of size "+(currentSizeOfItemsets+1));

	    }

	   
		private static void createItemsetsOfSize1() {
			String test = "a,b,c";
			
			String[] arr = test.split(",");
			numItems = arr.length;
			itemsets = new ArrayList<String[]>();
	        for(int i=0; i<numItems; i++)
	        {
	        	String[] cand = {arr[i]};
	        	itemsets.add(cand);
	        }

		}

}
