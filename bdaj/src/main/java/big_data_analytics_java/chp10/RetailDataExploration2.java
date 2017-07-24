package big_data_analytics_java.chp10;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RetailDataExploration2 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]");
		SparkSession session = SparkSession
								.builder()
								.config(conf)
							    .appName("Retail Data Exploration")
							    .getOrCreate();
		
		Dataset<Row> rawData = session.read().csv("data/retail/Online_Retail.csv");
		rawData.show();
		
		//only UK
		//Dataset<Row> rawDataUK = rawData.filter("_c7 = 'United Kingdom'");
		//	rawDataUK.show();
		
			JavaRDD<RetailVO> retailData = rawData.javaRDD().map(row -> {
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
				retailVO.setUnitPrice( Double.parseDouble(unitPrice) );
				retailVO.setCustomerID(customerID);
				if("United Kingdom".equals(country.trim())) retailVO.setCountry("UK");
				else retailVO.setCountry(country);
				retailVO.setQuantity(Integer.parseInt(quantity));
				
				return retailVO;
				
				//We will filter out any null value
			}).filter( rowObj -> null != rowObj);

		Dataset<Row> retailDS = session.createDataFrame(retailData.rdd(), RetailVO.class);
				retailDS.createOrReplaceTempView("retail");
				
		Dataset<Row> dataByCtryCnt = session.sql("select country,count(*) cnt from retail group by country having cnt > 1000");
		dataByCtryCnt.show();
		
		Dataset<Row> ukTransactions = session.sql("select * from retail where country = 'UK'");
			ukTransactions.describe("unitPrice","quantity").show();
	}
}
