package big_data_analytics_java.chp11.algos;

import java.util.ArrayList;
import java.util.List;

import big_data_analytics_java.chp11.flight_analytics.RelationVO;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;


public class BreadthFirstSearchBD {
   
	public static void main(String[] args) {
		SparkConf c = new SparkConf().setMaster("local");
		
		SparkSession session = SparkSession
				.builder()
				.config(c)
			    .appName("chp_5")
			    .getOrCreate();
		
		//vertexes
		List<UserVO> uList = new ArrayList<UserVO>() {
			{
				add(new UserVO("a", "Alice", 34));
				add(new UserVO("b", "Bob", 36));
				add(new UserVO("c", "Charlie", 30));
				add(new UserVO("d", "David", 29));
				add(new UserVO("e", "Esther", 32));
				add(new UserVO("f", "Fanny", 36));
				add(new UserVO("g", "Gabby", 60));
			}
		};
		
		Dataset<Row> verDF = session.createDataFrame(uList, UserVO.class);
		
		
		//edges
		List<RelationVO> edgeList = new ArrayList<RelationVO>() {
			{
				add(new RelationVO("a", "b", "friend"));
				add(new RelationVO("b", "c", "follow"));
				add(new RelationVO("c", "b", "follow"));
				add(new RelationVO("f", "c", "follow"));
				add(new RelationVO("e", "f", "follow"));
				add(new RelationVO("e", "d", "friend"));
				add(new RelationVO("d", "a", "friend"));
				add(new RelationVO("a", "e", "friend"));
			}
		};
		
		Dataset<Row> edgeDF = session.createDataFrame(edgeList, RelationVO.class);
		
		
		
		
		GraphFrame gf = new GraphFrame(verDF, edgeDF);
		
			gf.vertices().show();
			
		//bfs
	    Dataset<Row> breadthFirstResult = gf.bfs().fromExpr("id = 'a'").toExpr("id = 'e'").maxPathLength(1).run();
	    	breadthFirstResult.show();
    }
	
}
