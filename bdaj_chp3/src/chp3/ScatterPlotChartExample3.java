package chp3;

import org.jfree.chart.plot.PlotOrientation;

import java.awt.Color;
import java.util.List;

import javax.swing.Renderer;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.time.Month;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.time.TimeSeriesDataItem;
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
		inputData = createDataSet("data/housingprices.txt");
		chart = createChart(inputData);
		chart.getPlot().setBackgroundPaint(Color.WHITE);
		ChartPanel cPanel = new ChartPanel(chart);
			cPanel.setPreferredSize(new java.awt.Dimension(500,270));
			setContentPane(cPanel);
	}
	
	
	public static void main(String[] args) {
		ScatterPlotChartExample3 demo = new ScatterPlotChartExample3("Scatter Plot");
			demo.pack();
		RefineryUtilities.centerFrameOnScreen(demo);
			demo.setVisible(true);
	}
	
	
	private XYDataset createDataSet(String datasetFileName) {
		SparkConf sconf = new SparkConf() .setAppName(APP_NAME) .setMaster(APP_MASTER);
		JavaSparkContext sc = new JavaSparkContext(sconf);
		SQLContext sqlContext = new SQLContext(sc);
		JavaRDD<String> dataRows = sc.textFile(datasetFileName);
		JavaRDD<Double[]> dataRowArr = dataRows.map(new Function<String, Double[]>() {
		@Override
		public Double[] call(String line) throws Exception {
		String[] strs = line.split(",");
		Double[] arr = new Double[2];
			arr[0] = Double.parseDouble(strs[5]);
			arr[1] = Double.parseDouble(strs[2]);
			return arr;
		}
		}) ;
		List<Double[]> dataItems = dataRowArr.collect();
		XYSeriesCollection dataset = new XYSeriesCollection();
		XYSeries series = new XYSeries("Real estate item");
		for (Double[] darr : dataItems) {
		Double livingArea = darr[0];
		Double price = darr[1];
		series.add(livingArea, price);
		}
		dataset.addSeries(series);
			sc.stop();
		return dataset;
	}	
	
	
	private JFreeChart createChart(XYDataset inputDataSet) {
		JFreeChart chart1 = ChartFactory.createScatterPlot("price for Living Areal", "LivingArea", "Price", inputDataSet, PlotOrientation.VERTICAL, true, true, false);
		XYPlot plot = chart1.getXYPlot();
		XYItemRenderer renderer1 = plot.getRenderer();
		ValueAxis domain1 = new NumberAxis("Domain1");
		ValueAxis range1 = new NumberAxis("Range1");
		
			plot.setDataset(0, inputDataSet);
			plot.setRenderer(0, renderer1);
			plot.setDomainAxis(0, domain1);
			plot.setRangeAxis(0, range1);		
			plot.mapDatasetToDomainAxis(0, 0);
			plot.mapDatasetToRangeAxis(0, 0);

		XYDataset collection2 = createLineDataset();
		XYItemRenderer renderer2 = new XYLineAndShapeRenderer(true, false);
		ValueAxis domain2 = new NumberAxis("Domain2");
		ValueAxis range2 = new NumberAxis("Range2");
		
		plot.setDataset(1, collection2);
		plot.setRenderer(1, renderer2);
		plot.setDomainAxis(1, domain2);
		plot.setRangeAxis(1, range2);
		// Map the line to the second Domain and second Range
		plot.mapDatasetToDomainAxis(1, 1);
		plot.mapDatasetToRangeAxis(1, 1);
		// Create the chart with the plot and a legend
		JFreeChart chart = new JFreeChart("Multi Dataset Chart", JFreeChart.
		DEFAULT_TITLE_FONT, plot, true);
		return chart;
		}
	
	
	private XYDataset createLineDataset() {
		SparkConf sconf = new SparkConf() .setAppName(APP_NAME) .setMaster(APP_MASTER);
		JavaSparkContext sc = new JavaSparkContext(sconf);
		SQLContext sqlContext = new SQLContext(sc);
		JavaRDD<String> dataRows = sc.textFile("data/housingprices.txt");
		JavaRDD<Double[]> dataRowArr = dataRows.map(new Function<String, Double[]>() {
		@Override
		public Double[] call(String line) throws Exception {
		String[] strs = line.split(",");
		Double[] arr = new Double[2];
			arr[0] = 3* Double.parseDouble(strs[2]);
			arr[1] = Double.parseDouble(strs[2]);
			return arr;
		}
		}) ;
		List<Double[]> dataItems = dataRowArr.collect();
		XYSeriesCollection dataset = new XYSeriesCollection();
		XYSeries series = new XYSeries("Real estate item");
		for (Double[] darr : dataItems) {
		Double livingArea = darr[0];
		Double price = darr[1];
		series.add(livingArea, price);
		}
		dataset.addSeries(series);
			sc.stop();
		return dataset;
	}	
	
}