package big_data_analytics_java.chp9;

public class EuclidVO {
	private String movieId1;
	private String movieTitle1;
	private String movieId2;
	private String movieTitle2;
	private double euclidDist;
	
	public EuclidVO() {
		
	}


	public String getMovieTitle1() {
		return movieTitle1;
	}

	public void setMovieTitle1(String movieTitle1) {
		this.movieTitle1 = movieTitle1;
	}


	public String getMovieId1() {
		return movieId1;
	}


	public void setMovieId1(String movieId1) {
		this.movieId1 = movieId1;
	}


	public String getMovieId2() {
		return movieId2;
	}


	public void setMovieId2(String movieId2) {
		this.movieId2 = movieId2;
	}


	public String getMovieTitle2() {
		return movieTitle2;
	}

	public void setMovieTitle2(String movieTitle2) {
		this.movieTitle2 = movieTitle2;
	}

	public double getEuclidDist() {
		return euclidDist;
	}

	public void setEuclidDist(double euclidDist) {
		this.euclidDist = euclidDist;
	}
	
	
}
