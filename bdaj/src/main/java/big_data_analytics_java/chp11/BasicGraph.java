package big_data_analytics_java.chp11;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

public class BasicGraph {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		
		JavaRDD<Row> verRow =
				sc.parallelize(Arrays.asList(RowFactory.create(101L,"Kai",27),
				RowFactory.create(201L,"John",45),
				RowFactory.create(301L,"Alex",32),
				RowFactory.create(401L,"Tina",23)));		
		
		List<StructField> verFields = new ArrayList<StructField>();
			verFields.add(DataTypes.createStructField("id",DataTypes.LongType, true));
			verFields.add(DataTypes.createStructField("name",DataTypes.StringType, true));
			verFields.add(DataTypes.createStructField("age",DataTypes.IntegerType, true));
			
		JavaRDD<Row> edgRow = sc.parallelize(Arrays.asList(
					RowFactory.create(101L,301L,"Friends"),
					RowFactory.create(101L,401L,"Friends"),
					RowFactory.create(401L,201L,"Follow"),
					RowFactory.create(301L,201L,"Follow"),
					RowFactory.create(201L,101L,"Follow")));
		
		List<StructField> EdgFields = new ArrayList<StructField>();
		
		EdgFields.add(DataTypes.createStructField("src",DataTypes.LongType,true));
		EdgFields.add(DataTypes.createStructField("dst",DataTypes.LongType,true));
		EdgFields.add(DataTypes.createStructField("relationType",DataTypes.StringType,true));
		
		StructType verSchema = DataTypes.createStructType(verFields);
		StructType edgSchema = DataTypes.createStructType(EdgFields);
		
		Dataset<Row> verDF = sqlContext.createDataFrame(verRow, verSchema);
		Dataset<Row> edgDF = sqlContext.createDataFrame(edgRow, edgSchema);
		
		GraphFrame g = new GraphFrame(verDF,edgDF);
		
		g.edges().show();	
		
		System.out.println("Number of Vertices : " + g.vertices().count());
		System.out.println("Number of Edges : " + g.edges().count());
		
		g.inDegrees().show();
		
		//g.inDegrees().filter("inDegree > 1").show();
		
		g.vertices().filter("age > 30").show();
	}
	
}
