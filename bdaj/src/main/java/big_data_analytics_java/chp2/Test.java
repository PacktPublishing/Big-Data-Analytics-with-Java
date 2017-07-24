package big_data_analytics_java.chp2;

import java.util.Arrays;

public class Test {

	public static void main(String[] args) {
		   String[] s = {"cabc","aab","bkdf","aab"};
		    Arrays.sort(s);
		    for (String string : s) {
				System.out.println(string);
			}
		    String[] test = "lkasdf".split(",");
		    System.out.println(test[0]);;
	}

}
