package org.myorg;

public class Movie {
	private int movieId;
	private String movieTitle;
	private String genres;
	
	public Movie(int movieId, String movieTitle, String genres) {
		super();
		this.movieId = movieId;
		this.movieTitle = movieTitle;
		this.genres = genres;
	}
	
	public int getMovieId() {
		return movieId;
	}
	public String getMovieTitle() {
		return movieTitle;
	}
	public String getGenres() {
		return genres;
	}
}
