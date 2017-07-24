package big_data_analytics_java.chp10;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.clustering.BisectingKMeans;
import org.apache.spark.ml.clustering.BisectingKMeansModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.VectorAssembler;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
/*
 * Using Kmeans clustering for customer segmentation.
 */
public class BisectingKmeansClustering {
 
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]");
		SparkSession session = SparkSession
								.builder()
								.config(conf)
							    .appName("kmeans")
							    .getOrCreate();
		
		Dataset<Row> rawData = session.read().csv("data/retail/Online_Retail.csv");
		rawData.show();
		
		//only UK
		Dataset<Row> rawDataUK = rawData.filter("_c7 = 'United Kingdom'");
		//	rawDataUK.show();
		
			JavaRDD<RetailVO> retailData = rawDataUK.javaRDD().map(row -> {
				RetailVO retailVO = new RetailVO();
				// data cleanser
				// remove data with missing entries
				String invoiceNo = row.getString(0);
				String stockCode = row.getString(1);
				String description = row.getString(2);
				String quantity = row.getString(3);
				String invoiceDate = row.getString(4);
				String unitPrice = row.getString(5);
				String customerID = row.getString(6);
				String country = row.getString(7);
				
				if( null == invoiceNo || "".equals(invoiceNo.trim()) ||
				    null == stockCode || "".equals(stockCode.trim()) || 
				    null == description || "".equals(description.trim()) ||
				    null == quantity || "".equals(quantity.trim()) ||
				    null == invoiceDate || "".equals(invoiceDate.trim()) ||
				    null == unitPrice || "".equals(unitPrice.trim()) ||
				    null == customerID || "".equals(customerID.trim()) ||
				    null == country || "".equals(country.trim())) {
					return null;
				}
				
				retailVO.setInvoiceNo(invoiceNo);
				retailVO.setStockCode(stockCode);
				retailVO.setDescription(description);
				retailVO.setInvoiceDate(invoiceDate);
				Double unitPriceDbl = Double.parseDouble(unitPrice);
					retailVO.setUnitPrice( unitPriceDbl  );
				retailVO.setCustomerID(customerID);
				retailVO.setCountry(country);
				Integer quantityInt = Integer.parseInt(quantity); 
					retailVO.setQuantity(quantityInt);
				
				long amountSpent = Math.round(unitPriceDbl * quantityInt );
					retailVO.setAmountSpent(amountSpent);
				
				//Recency
				SimpleDateFormat myFormat = new SimpleDateFormat("MM/dd/yyyy");
				Date date1 = myFormat.parse(invoiceDate);
				Date date2 = myFormat.parse("12/31/2012");
					long diff = date2.getTime() - date1.getTime();
					long days = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
					
						retailVO.setRecency(days);
					
				return retailVO;
				
				//We will filter out any null value
			}).filter( rowObj -> null != rowObj);

		Dataset<Row> retailDS = session.createDataFrame(retailData.rdd(), RetailVO.class);
					 retailDS.show();
				retailDS.createOrReplaceTempView("transactions");
		
		//Recency
		Dataset<Row> recencyDS = session.sql("select customerID,min(recency) recency from transactions group by customerID");
					 recencyDS.createOrReplaceTempView("recencyData");
					 recencyDS.show();
				
	    //frequency
		Dataset<Row> freqDS = session.sql("select customerID,count(*) frequency from transactions group by customerID");
					 freqDS.createOrReplaceTempView("freqData");
					 freqDS.show();
		//Monetary
		Dataset<Row> monetoryDS = session.sql("select customerID,sum(amountSpent) spending from transactions group by customerID");
					 monetoryDS.createOrReplaceTempView("spendingData");					 
					 monetoryDS.show();
					 
		
		Dataset<Row> resultDS = session.sql("select r.customerID, r.recency, f.frequency, s.spending from recencyData r, freqData f, spendingData s "
				+ "where r.customerID = f.customerID and f.customerID = s.customerID");
					
				     resultDS.show();
				     
		VectorAssembler assembler = new VectorAssembler().setInputCols(new String[] {"recency","frequency","spending"}).setOutputCol("features");
		
		Dataset<Row> datasetWithFeatures = assembler.transform(resultDS);
					 datasetWithFeatures.show(7);
					 
		Normalizer normalizer = new Normalizer().setInputCol("features").setOutputCol("normFeatures");
		
		Dataset<Row> normDataset = normalizer.transform(datasetWithFeatures);
					 normDataset.show(10);
					 normDataset.createOrReplaceTempView("norm_data");
		
			//datasetWithFeatures.describe("recency","frequency","spending").show();
		
//		//Kmeans
//		KMeans km = new KMeans().setK(5).setSeed(1L).setFeaturesCol("normFeatures");
//		
//		KMeansModel kmodel = km.fit(normDataset);
	
//		Dataset<Row> clusters = kmodel.transform(normDataset);
			
		BisectingKMeans bkm = new BisectingKMeans().setK(5).setSeed(1L).setFeaturesCol("normFeatures");
		BisectingKMeansModel bkmodel = bkm.fit(normDataset);

		Dataset<Row> clusters = bkmodel.transform(normDataset);		
				clusters.show();
		
					 clusters.createOrReplaceTempView("clusters");
					 
		session.sql("select prediction,count(*) from clusters group by prediction").show();
		
		//session.sql("select * from norm_data where customerID in (select customerID from clusters where prediction = 2)").show();
		//Dataset<Row> resultDS = session.sql(sqlText)
					 
		//|536365|85123A|WHITE HANGING HEA...|  6|12/1/2010 8:26|2.55|17850|United Kingdom|
	}

}
