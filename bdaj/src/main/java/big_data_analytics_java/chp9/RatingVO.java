package big_data_analytics_java.chp9;

import java.io.Serializable;

public class RatingVO implements Serializable {
    private int userId;
    private int movieId;
    private float rating;
    private long timestamp;
    private int like; //1 for like , 0 for not

    public RatingVO() {}

    
    public RatingVO(int userId, int movieId, float rating, long timestamp) {
      this.userId = userId;
      this.movieId = movieId;
      this.rating = rating;
      this.timestamp = timestamp;
    }

    
    public RatingVO(int userId, int movieId, float rating, long timestamp,int like) {
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
        this.timestamp = timestamp;
        this.like = like;
    }    
    
    
    public int getUserId() {
      return userId;
    }

    
    public int getMovieId() {
      return movieId;
    }

    
    public float getRating() {
      return rating;
    }

    
    public long getTimestamp() {
      return timestamp;
    }

    
    public static RatingVO parseRating(String str) {
      String[] fields = str.split("\t");
      if (fields.length != 4) {
        throw new IllegalArgumentException("Each line must contain 4 fields");
      }
      int userId = Integer.parseInt(fields[0]);
      int movieId = Integer.parseInt(fields[1]);
      float rating = Float.parseFloat(fields[2]);
      long timestamp = Long.parseLong(fields[3]);
      
      if(rating > 3) return new RatingVO(userId, movieId, rating, timestamp,1);
      return new RatingVO(userId, movieId, rating, timestamp,0);
    }


	public int getLike() {
		return like;
	}


	public void setLike(int like) {
		this.like = like;
	}



    
    
    
  }

