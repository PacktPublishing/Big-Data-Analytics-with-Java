package big_data_analytics_java.chp2;

import java.io.Serializable;

public class Rule implements Serializable{
	private String lhs;
	private String rhs;
	private double supportLhs;
	private double supportRhs;
	private double confidence;
	public String getLhs() {
		return lhs;
	}
	public void setLhs(String lhs) {
		this.lhs = lhs;
	}
	public String getRhs() {
		return rhs;
	}
	public void setRhs(String rhs) {
		this.rhs = rhs;
	}
	public double getSupportLhs() {
		return supportLhs;
	}
	public void setSupportLhs(double supportLhs) {
		this.supportLhs = supportLhs;
	}
	public double getSupportRhs() {
		return supportRhs;
	}
	public void setSupportRhs(double supportRhs) {
		this.supportRhs = supportRhs;
	}
	public double getConfidence() {
		return confidence;
	}
	public void setConfidence(double confidence) {
		this.confidence = confidence;
	}
	
	
}
