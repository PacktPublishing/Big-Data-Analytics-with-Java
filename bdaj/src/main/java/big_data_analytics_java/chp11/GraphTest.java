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

public class GraphTest {
	
	public static void main(String[] args ) {
		SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		JavaRDD<Row> verRow =
				sc.parallelize(Arrays.asList(RowFactory.create(101L,"Trina",27),
				RowFactory.create(201L,"Raman",45),
				RowFactory.create(301L,"Ajay",32),
				RowFactory.create(401L,"Sima",23)));
		
		List<StructField> verFields = new ArrayList<StructField>();
		verFields.add(DataTypes.createStructField("id",DataTypes.LongType, true));
		verFields.add(DataTypes.createStructField("name",DataTypes.StringType, true));
		verFields.add(DataTypes.createStructField("age",DataTypes.IntegerType, true));		
		
		JavaRDD<Row> edgRow = sc.parallelize(Arrays.asList(
				 RowFactory.create(101L,301L,"Colleague"),
				 RowFactory.create(101L,401L,"Friends"),
				 RowFactory.create(401L,201L,"Reports"),
				 RowFactory.create(301L,201L,"Reports"),
				 RowFactory.create(201L,101L,"Reports")));
		
		List<StructField> EdgFields = new ArrayList<StructField>();
		EdgFields.add(DataTypes.createStructField("src",DataTypes.LongType,
		true));
		EdgFields.add(DataTypes.createStructField("dst",DataTypes.LongType,
		true));
		EdgFields.add(DataTypes.createStructField("relationType",DataTypes.StringType,
		true));
		
		StructType verSchema = DataTypes.createStructType(verFields);
		StructType edgSchema = DataTypes.createStructType(EdgFields);

		Dataset verDF = sqlContext.createDataFrame(verRow, verSchema);
		Dataset edgDF = sqlContext.createDataFrame(edgRow, edgSchema);
		System.out.println("finished");
		
		GraphFrame g = new GraphFrame(verDF,edgDF);
		g.vertices().show();
		
	}
}
