package chp8;

import java.awt.Color;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
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
public class ScatterPlotChartExample3 extends ApplicationFrame {
	XYDataset inputData;
	JFreeChart chart;
	public static String APP_NAME = "SCATTER_PLOT_EXAMPLE";
	public static String APP_MASTER = "local";
	
	public ScatterPlotChartExample3(String title) {
		super(title);
		inputData = createDataSet();
			chart = createChart(inputData);
			chart.getPlot().setBackgroundPaint(Color.WHITE);
		ChartPanel cPanel = new ChartPanel(chart);
			cPanel.setPreferredSize(new java.awt.Dimension(500,270));
				setContentPane(cPanel);
	}
	
	public static void main(String[] args) {
		ScatterPlotChartExample3 demo = new ScatterPlotChartExample3 ("Scatter Plot");
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
	    
		
	    JavaRDD<String> dataRdd = 
	    		spark.sparkContext().textFile("C:/harpreet/datasets/LoanStats3a.csv",1).toJavaRDD();
	    
	    //1. For our Analysis we will filter all teh 'current' loans
	    JavaRDD<String> filteredLoans = dataRdd.filter( row -> {
	    	return !row.contains("Current");
	    });
	    
	    JavaRDD<LoanVO> data = filteredLoans.map( r -> {
	    	String[] darr = r.split(",");
	    	if(darr.length < 100) return null;
	    	LoanVO lvo = new LoanVO();
	    		   lvo.setLoanAmt(Double.parseDouble(darr[2].replaceAll("\"", "").trim()));
	    		   lvo.setLoanId(Integer.parseInt(darr[0].replaceAll("\"", "").trim()));
	    		   lvo.setFundedAmt(Double.parseDouble(darr[3].replaceAll("\"", "").trim()));
	    		   lvo.setFundedAmtInv(Double.parseDouble(darr[4].replaceAll("\"", "").trim()));
	    		   lvo.setGrade(darr[8].replaceAll("\"", "").trim());
	    		   lvo.setSubGrade(darr[9].replaceAll("\"", "").trim());
	    		   String empLength = darr[11].replaceAll("\"", "").trim();
	    		   lvo.setEmpLengthStr(empLength);
	    		   String empLengthStr = empLength.replaceAll("\\>","").replaceAll("\\<","").replaceAll("Feel Fine","").replaceAll("year","").replaceAll("\\+","").replaceAll("s","").replaceAll("Inc","").replaceAll("Inc.","").replaceAll("LLC","").trim();
	    		   if("".equals(empLengthStr)) return null;
	    		   lvo.setEmpLength(Integer.parseInt(empLengthStr));
	    		   lvo.setHomeOwnership(darr[12].replaceAll("\"", "").trim());
	    		   lvo.setAnnualInc(Double.parseDouble(darr[2].replaceAll("\"", "").trim()));
	    		   String loanStatus = darr[16].replaceAll("\"", "").trim();
	    		   lvo.setLoanStatusStr(loanStatus);
	    		   
	    		   if(loanStatus.contains("Fully")) lvo.setLoanStatus(1.0);
	    		   else lvo.setLoanStatus(0.0);
	    		   
	    		   lvo.setLoanDesc(darr[20].replaceAll("\"", "").trim());
	    		   lvo.setTitle(darr[22].replaceAll("\"", "").trim());
	    		   lvo.setZipCode(darr[23].replaceAll("\"", "").trim());
	    		   
	    		   return lvo;

	    } ).filter(f ->  {
	    	if(f == null) return false;
	    	else return true;
	    });

	    JavaRDD<Double[]> dataRowArr = data.map(r -> {
			Double[] arr = new Double[2];
			arr[0] = Double.parseDouble(r.getEmpLength().toString());
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