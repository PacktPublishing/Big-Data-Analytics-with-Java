package chp8;

import java.awt.Color;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
public class ScatterPlotChartExample2 extends ApplicationFrame {
	XYDataset inputData;
	JFreeChart chart;
	public static String APP_NAME = "SCATTER_PLOT_EXAMPLE";
	public static String APP_MASTER = "local";
	
	public ScatterPlotChartExample2(String title) {
		super(title);
		inputData = createDataSet();
			chart = createChart(inputData);
			chart.getPlot().setBackgroundPaint(Color.WHITE);
		ChartPanel cPanel = new ChartPanel(chart);
			cPanel.setPreferredSize(new java.awt.Dimension(500,270));
				setContentPane(cPanel);
	}
	
	public static void main(String[] args) {
		ScatterPlotChartExample2 demo = new ScatterPlotChartExample2 ("Scatter Plot");
			demo.pack ();
		RefineryUtilities.centerFrameOnScreen(demo);
			demo.setVisible(true);
	}
	
	
	private JFreeChart createChart(XYDataset inputDataSet) {
		JFreeChart chart = ChartFactory.createScatterPlot("Funded Loan Amount vs Annual Income", "Annual Income", "Funded Amount", inputDataSet, PlotOrientation.VERTICAL, true, true, false);
		XYPlot plot = chart.getXYPlot();
		plot.getRenderer().setSeriesPaint(0, Color.blue);
		return chart;
	}

	
	private XYDataset createDataSet() {
		SparkConf c = new SparkConf().setMaster("local[*]");
	    
		SparkSession spark = SparkSession
	      .builder()
	      .config(c)
	      .appName("LoanLendingTree")
	      .getOrCreate();
	    
		
	    Dataset<Row> defaultData = spark.read().csv("C:/harpreet/datasets/LoanStats3a.csv");
		 // defaultData.show();

JavaRDD<Row> rdd = defaultData.toJavaRDD();

JavaRDD<LoanVO> data = rdd.map( r -> {
	if(r.size() < 100) return null;
	LoanVO lvo = new LoanVO();
	String loanId = r.getString(0).trim();
//	System.out.println("loanId --> " + loanId);
	String loanAmt = r.getString(2).trim();
//	System.out.println("loanId --> " + loanId);;
//	System.out.println("localid --> " + loanAmt.trim());;
//	System.out.println("loanId --> " + loanId);
	String fundedAmt = r.get(3).toString().trim();
	String grade = r.get(8).toString().trim();
	String subGrade = r.get(9).toString().trim();
	String empLength = r.get(11).toString().trim();
	String homeOwn = r.get(12).toString().trim();
	String annualInc = r.getString(13);
	String loanStatus = r.get(16).toString().trim();
				
	if(null == annualInc || "".equals(annualInc) || 
			null == loanAmt || "".equals(loanAmt) || 
			null == grade || "".equals(grade) || 
			null == subGrade || "".equals(subGrade) || 
			null == empLength || "".equals(empLength) || 
			null == homeOwn || "".equals(homeOwn) || 
			null == loanStatus || "".equals(loanStatus)) return null;
	
	
	if(loanAmt.contains("N/A") || loanId.contains("N/A") || fundedAmt.contains("N/A") || grade.contains("N/A") ||
			subGrade.contains("N/A") || empLength.contains("N/A") || homeOwn.contains("N/A") || annualInc.contains("N/A") || loanStatus.contains("N/A")) 
		return null;
	
	
	
	if("Current".equalsIgnoreCase(loanStatus)) return null;
	
		   lvo.setLoanAmt(Double.parseDouble(loanAmt));
		   lvo.setLoanId(Integer.parseInt(loanId));
		   lvo.setFundedAmt(Double.parseDouble(fundedAmt));
	//	   lvo.setFundedAmtInv(Double.parseDouble(darr[4].trim()));
		   lvo.setGrade(grade);
		   lvo.setSubGrade(subGrade);
		   lvo.setEmpLengthStr(empLength);
		   lvo.setHomeOwnership(homeOwn);
		   double annualIncDbl = Double.parseDouble(annualInc.trim());
		   if(annualIncDbl > 200000) return null;
		   lvo.setAnnualInc(annualIncDbl);
		   
		   lvo.setLoanStatusStr(loanStatus);
		   
		   if(loanStatus.contains("Fully")) lvo.setLoanStatus(1.0);
		   else lvo.setLoanStatus(0.0);
		   
	//	   lvo.setLoanDesc(darr[20].trim());
	//	   lvo.setTitle(darr[22].trim());
	//	   lvo.setZipCode(darr[23].trim());
		   
		   return lvo;
	
	} ).filter(f ->  {
	if(f == null) return false;
	else return true;
	});



	    JavaRDD<Double[]> dataRowArr = data.map(r -> {
			Double[] arr = new Double[2];
			arr[0] = r.getAnnualInc();
			arr[1] = r.getFundedAmt();
			return arr;
	    });

		
		List<Double[]> dataltems = dataRowArr.collect();
		XYSeriesCollection dataset =new XYSeriesCollection() ;
		XYSeries series= new XYSeries("Funded Loan Amount vs Annual Income of applicant");
		for (Double[] darr : dataltems) {
			Double livingArea = darr[0];
			Double price = darr[1];
			series.add(livingArea, price);
		}
			dataset.addSeries(series);
			return dataset;
	}

	
	// Here we consider the lower percent as 25% and highest as 75%
	public static double quartile(double[] values, double lowerPercent) {
		if (values == null || values.length == 0) {
				throw new IllegalArgumentException("The data array either is null or does not contain any data. ");
		}
		// Rank order the values
	
		double[] v = new double[values.length];
			System.arraycopy(values, 0, v, 0, values.length);
			Arrays.sort(v);
			int n = (int) Math. round (v.length * lowerPercent / 100);
			return v[n];
	}
	
	
}