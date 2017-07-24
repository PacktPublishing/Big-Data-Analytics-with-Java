package big_data_analytics_java.chp7;

//Loan_ID,Gender,Married,Dependents,Education,Self_Employed,ApplicantIncome,CoapplicantIncome,LoanAmount,Loan_Amount_Term,Credit_History,Property_Area,Loan_Status
public class LoanVO {
	private double gender;
	private double married;
	private double education;
	private double applicantIncome;
	private double loanAmount;
	private double loanAmountTerm;
	private double creditHistory;
	private double loanStatus;
	
	public LoanVO() {
		
	}

	public double getGender() {
		return gender;
	}

	public void setGender(double gender) {
		this.gender = gender;
	}

	public double getMarried() {
		return married;
	}

	public void setMarried(double married) {
		this.married = married;
	}

	public double getEducation() {
		return education;
	}

	public void setEducation(double education) {
		this.education = education;
	}

	public double getApplicantIncome() {
		return applicantIncome;
	}

	public void setApplicantIncome(double applicantIncome) {
		this.applicantIncome = applicantIncome;
	}

	public double getLoanAmount() {
		return loanAmount;
	}

	public void setLoanAmount(double loanAmount) {
		this.loanAmount = loanAmount;
	}

	public double getLoanAmountTerm() {
		return loanAmountTerm;
	}

	public void setLoanAmountTerm(double loanAmountTerm) {
		this.loanAmountTerm = loanAmountTerm;
	}

	public double getCreditHistory() {
		return creditHistory;
	}

	public void setCreditHistory(double creditHistory) {
		this.creditHistory = creditHistory;
	}

	public double getLoanStatus() {
		return loanStatus;
	}

	public void setLoanStatus(double loanStatus) {
		this.loanStatus = loanStatus;
	}
	
	
}
