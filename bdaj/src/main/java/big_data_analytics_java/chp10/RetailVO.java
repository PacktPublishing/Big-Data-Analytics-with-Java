package big_data_analytics_java.chp10;

public class RetailVO {
 
	private String invoiceNo;
	private String stockCode;
	private String description;
	private Integer quantity;
	private String invoiceDate;
	private Double unitPrice;
	private String customerID;
	private String Country;
	private long amountSpent;
	private long recency;
 
 	public RetailVO() {
	 
 	}

	public String getInvoiceNo() {
		return invoiceNo;
	}

	public void setInvoiceNo(String invoiceNo) {
		this.invoiceNo = invoiceNo;
	}

	public String getStockCode() {
		return stockCode;
	}

	public void setStockCode(String stockCode) {
		this.stockCode = stockCode;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Integer getQuantity() {
		return quantity;
	}

	public void setQuantity(Integer quantity) {
		this.quantity = quantity;
	}

	public String getInvoiceDate() {
		return invoiceDate;
	}

	public void setInvoiceDate(String invoiceDate) {
		this.invoiceDate = invoiceDate;
	}

	public Double getUnitPrice() {
		return unitPrice;
	}

	public void setUnitPrice(Double unitPrice) {
		this.unitPrice = unitPrice;
	}

	public String getCustomerID() {
		return customerID;
	}

	public void setCustomerID(String customerID) {
		this.customerID = customerID;
	}

	public String getCountry() {
		return Country;
	}

	public void setCountry(String country) {
		Country = country;
	}

	public long getAmountSpent() {
		return amountSpent;
	}

	public void setAmountSpent(long amountSpent) {
		this.amountSpent = amountSpent;
	}

	public long getRecency() {
		return recency;
	}

	public void setRecency(long recency) {
		this.recency = recency;
	}



	 
 
}
