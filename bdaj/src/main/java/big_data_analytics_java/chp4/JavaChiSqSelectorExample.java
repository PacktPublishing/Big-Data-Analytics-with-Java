package big_data_analytics_java.chp4;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

// $example on$
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.ml.feature.ChiSqSelector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$

public class JavaChiSqSelectorExample {
  public static void main(String[] args) {
	SparkConf c = new SparkConf().setMaster("local[*]");
    SparkSession spark = SparkSession
      .builder()
      .config(c)
      .appName("JavaChiSqSelectorExample")
      .getOrCreate();

    // $example on$
    List<Row> data = Arrays.asList(
      RowFactory.create(7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
      RowFactory.create(8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
      RowFactory.create(9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
    );
    StructType schema = new StructType(new StructField[]{
      new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("features", new VectorUDT(), false, Metadata.empty()),
      new StructField("clicked", DataTypes.DoubleType, false, Metadata.empty())
    });

    Dataset<Row> df = spark.createDataFrame(data, schema);

    ChiSqSelector selector = new ChiSqSelector()
      .setNumTopFeatures(1)
      .setFeaturesCol("features")
      .setLabelCol("clicked")
      .setOutputCol("selectedFeatures");

    Dataset<Row> result = selector.fit(df).transform(df);

    System.out.println("ChiSqSelector output with top " + selector.getNumTopFeatures()
        + " features selected");
    result.show();

    // $example off$
    spark.stop();
  }
}

