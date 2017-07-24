package chp7;

import java.awt.Font;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.labels.BoxAndWhiskerToolTipGenerator;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.renderer.category.BoxAndWhiskerRenderer;
import org.jfree.data.statistics.BoxAndWhiskerCategoryDataset;
import org.jfree.data.statistics.DefaultBoxAndWhiskerCategoryDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;
import org.jfree.util.Log;
import org.jfree.util.LogContext;


public class BoxChartAppIncomeExample extends ApplicationFrame {

    /** Access to logging facilities. */
    private static final LogContext LOGGER = Log.createContext(BoxChartAppIncomeExample.class);

    /**
     * Creates a new demo.
     *
     * @param title  the frame title.
     */
    public BoxChartAppIncomeExample(final String title) {

        super(title);
        
        final BoxAndWhiskerCategoryDataset dataset = createSampleDataset();

        final CategoryAxis xAxis = new CategoryAxis("All users");
        final NumberAxis yAxis = new NumberAxis("Loan Amount");
        yAxis.setAutoRangeIncludesZero(false);
        final BoxAndWhiskerRenderer renderer = new BoxAndWhiskerRenderer();
        renderer.setFillBox(true);
        renderer.setToolTipGenerator(new BoxAndWhiskerToolTipGenerator());
        renderer.setMeanVisible(false);
        final CategoryPlot plot = new CategoryPlot(dataset, xAxis, yAxis, renderer);

        final JFreeChart chart = new JFreeChart(
            "Box chart for loan amount",
            new Font("SansSerif", Font.BOLD, 14),
            plot,
            true
        );
        final ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(450, 270));
        setContentPane(chartPanel);

    }

    /**
     * Creates a sample dataset.
     * 
     * @return A sample dataset.
     */
//    private BoxAndWhiskerCategoryDataset createSampleDataset() {
//        
//        final int seriesCount = 3;
//        final int categoryCount = 4;
//        final int entityCount = 22;
//        
//        final DefaultBoxAndWhiskerCategoryDataset dataset 
//            = new DefaultBoxAndWhiskerCategoryDataset();
//        for (int i = 0; i < seriesCount; i++) {
//            for (int j = 0; j < categoryCount; j++) {
//                final List list = new ArrayList();
//                // add some values...
//                for (int k = 0; k < entityCount; k++) {
//                    final double value1 = 10.0 + Math.random() * 3;
//                    list.add(new Double(value1));
//                    final double value2 = 11.25 + Math.random(); // concentrate values in the middle
//                    list.add(new Double(value2));
//                }
//                LOGGER.debug("Adding series " + i);
//                LOGGER.debug(list.toString());
//                dataset.add(list, "Series " + i, " Type " + j);
//            }
//            
//        }
//
//        return dataset;
//    }
    
    private BoxAndWhiskerCategoryDataset createSampleDataset() {
	    System.out.println(avgLoanAmt);
	    List<Row> loanAmtsList = loanAmtDS.collectAsList();

    	List list1 = new ArrayList<Double>();
	    for (Row row : loanAmtsList) {
	    	if( null != row.getString(0) && 
	    			 !"".equals(row.getString(0))) list1.add(Double.parseDouble( row.getString(0)) ); 	    	
		}    	

    	List list2 = new ArrayList<Double>();
	    for (Row row : loanAmtsList) {
	    	if( null == row.getString(0) || "".equals(row.getString(0))) list2.add(avgLoanAmt);
	    	else list2.add(Double.parseDouble( row.getString(0)) );
		} 
	    
    	final DefaultBoxAndWhiskerCategoryDataset dataset = new DefaultBoxAndWhiskerCategoryDataset();
   			  dataset.add(list1, "Loan Amount Box Chart", "chart 1");
   			  dataset.add(list2, "Loan Amount Box Chart", "chart 2");
    		return dataset;
    }
    		
    	

    // ****************************************************************************
    // * JFREECHART DEVELOPER GUIDE                                               *
    // * The JFreeChart Developer Guide, written by David Gilbert, is available   *
    // * to purchase from Object Refinery Limited:                                *
    // *                                                                          *
    // * http://www.object-refinery.com/jfreechart/guide.html                     *
    // *                                                                          *
    // * Sales are used to provide funding for the JFreeChart project - please    * 
    // * support us so that we can continue developing free software.             *
    // ****************************************************************************
    
    /**
     * For testing from the command line.
     *
     * @param args  ignored.
     */
    public static void main(final String[] args) {

        //Log.getInstance().addTarget(new PrintStreamLogTarget(System.out));
        final BoxChartAppIncomeExample demo = new BoxChartAppIncomeExample("Loan Amount as per this chart");
        demo.pack();
        RefineryUtilities.centerFrameOnScreen(demo);
        demo.setVisible(true);

    }

}
