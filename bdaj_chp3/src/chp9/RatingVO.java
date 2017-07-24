package chp9;

import java.io.Serializable;

public class RatingVO implements Serializable {
    private int userId;
    private int movieId;
    private float rating;
    private long timestamp;

    public RatingVO() {}

    public RatingVO(int userId, int movieId, float rating, long timestamp) {
      this.userId = userId;
      this.movieId = movieId;
      this.rating = rating;
      this.timestamp = timestamp;
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
      return new RatingVO(userId, movieId, rating, timestamp);
    }
  }

