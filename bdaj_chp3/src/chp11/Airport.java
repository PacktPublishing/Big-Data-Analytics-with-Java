package chp11;

public class Airport {
	
	private String airportId;
	private String id;
	private String airportIataCode;
	private String airportIcaoCode;
	private String airportName;
	private String latitude;
	private String longitude;
	private String country;
	private String state;
	
//	3494,"Newark Liberty International Airport","Newark","United States","EWR","KEWR",40.692501068115234,-74.168701171875,18,-5,"A","America/New_York","airport","OurAirports"
	public String getAirportId() {
		return airportId;
	}
	public void setAirportId(String airportId) {
		this.airportId = airportId;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getAirportIataCode() {
		return airportIataCode;
	}
	public void setAirportIataCode(String airportIataCode) {
		this.airportIataCode = airportIataCode;
	}
	public String getAirportIcaoCode() {
		return airportIcaoCode;
	}
	public void setAirportIcaoCode(String airportIcaoCode) {
		this.airportIcaoCode = airportIcaoCode;
	}
	public String getAirportName() {
		return airportName;
	}
	public void setAirportName(String airportName) {
		this.airportName = airportName;
	}
	public String getLatitude() {
		return latitude;
	}
	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}
	public String getLongitude() {
		return longitude;
	}
	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	
	
	
}
