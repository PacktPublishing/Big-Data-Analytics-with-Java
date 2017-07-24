package big_data_analytics_java.chp8;

import java.io.Serializable;

/*
  * pojo
  */
public class LoanVO implements Serializable{
	
   private Integer loanId;
   private Double loanAmt;
   private Double fundedAmt;
   private Double fundedAmtInv;
   private String grade;
   private String subGrade;
   private String empLengthStr;
   private Integer empLength;
   private String homeOwnership;
   private Double annualInc;
   private Double loanStatus;
   private String loanDesc;
   private String title;
   private String zipCode;
   private String loanStatusStr;
   private String loanAmtStr;
   
   public LoanVO() {
	   
   }

	public Integer getLoanId() {
		return loanId;
	}
	
	public void setLoanId(Integer loanId) {
		this.loanId = loanId;
	}
	
	public Double getLoanAmt() {
		return loanAmt;
	}
	
	public void setLoanAmt(Double loanAmt) {
		this.loanAmt = loanAmt;
	}
	
	public Double getFundedAmt() {
		return fundedAmt;
	}
	
	public void setFundedAmt(Double fundedAmt) {
		this.fundedAmt = fundedAmt;
	}
	
	public Double getFundedAmtInv() {
		return fundedAmtInv;
	}
	
	public void setFundedAmtInv(Double fundedAmtInv) {
		this.fundedAmtInv = fundedAmtInv;
	}
	
	public String getGrade() {
		return grade;
	}
	
	public void setGrade(String grade) {
		this.grade = grade;
	}
	
	public String getSubGrade() {
		return subGrade;
	}
	
	public void setSubGrade(String subGrade) {
		this.subGrade = subGrade;
	}
	
	public String getEmpLengthStr() {
		return empLengthStr;
	}
	
	public void setEmpLengthStr(String empLengthStr) {
		this.empLengthStr = empLengthStr;
	}
	
	public Integer getEmpLength() {
		return empLength;
	}
	
	public void setEmpLength(Integer empLength) {
		this.empLength = empLength;
	}
	
	public String getHomeOwnership() {
		return homeOwnership;
	}
	
	public void setHomeOwnership(String homeOwnership) {
		this.homeOwnership = homeOwnership;
	}
	
	public Double getAnnualInc() {
		return annualInc;
	}
	
	public void setAnnualInc(Double annualInc) {
		this.annualInc = annualInc;
	}
	
	public Double getLoanStatus() {
		return loanStatus;
	}
	
	public void setLoanStatus(Double loanStatus) {
		this.loanStatus = loanStatus;
	}
	
	public String getLoanDesc() {
		return loanDesc;
	}
	
	public void setLoanDesc(String loanDesc) {
		this.loanDesc = loanDesc;
	}
	
	public String getTitle() {
		return title;
	}
	
	public void setTitle(String title) {
		this.title = title;
	}
	
	public String getZipCode() {
		return zipCode;
	}
	
	public void setZipCode(String zipCode) {
		this.zipCode = zipCode;
	}

	public String getLoanStatusStr() {
		return loanStatusStr;
	}

	public void setLoanStatusStr(String loanStatusStr) {
		this.loanStatusStr = loanStatusStr;
	}

	public String getLoanAmtStr() {
		return loanAmtStr;
	}

	public void setLoanAmtStr(String loanAmtStr) {
		this.loanAmtStr = loanAmtStr;
	}
	  
	
	   
}
