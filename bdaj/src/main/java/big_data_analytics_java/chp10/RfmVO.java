package big_data_analytics_java.chp10;

/*
 * Recency
 * Frequency
 * Monetary
 * Spending
 * 
 */
public class RfmVO {
	private int recency;
	private int frequency;
	private double monetary;
	private double spending;
	
	public RfmVO() {
		
	}

	public int getFrequency() {
		return frequency;
	}

	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}

	public double getMonetary() {
		return monetary;
	}

	public void setMonetary(double monetary) {
		this.monetary = monetary;
	}

	public int getRecency() {
		return recency;
	}

	public void setRecency(int recency) {
		this.recency = recency;
	}

	public double getSpending() {
		return spending;
	}

	public void setSpending(double spending) {
		this.spending = spending;
	}
	
	
	
}
