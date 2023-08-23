package org.myorg;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.TimeZone;

public class Rating {
	private int movieId;
	private float movieRating;
	private int month;
	
	public Rating(int movieId, float movieRating, int month) {
		super();
		this.movieId = movieId;
		this.movieRating = movieRating;
		this.month = month;
	}
	
	public int getMovieId() {
		return movieId;
	}
	public float getMovieRating() {
		return movieRating;
	}

	public int getMonth() {
		return month;
	}
	
	public static int calculateMonth(String timestamp) {
		LocalDate ratingDate = getDateFromTimestamp(Long.parseLong(timestamp));
		return ratingDate.getMonthValue();
	}
	
	public static LocalDateTime getDateTimeFromTimestamp(long timestamp) {
		if (timestamp == 0)
			return null;
		return LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp), TimeZone
				.getDefault().toZoneId());
	}

	public static LocalDate getDateFromTimestamp(long timestamp) {
		LocalDateTime date = getDateTimeFromTimestamp(timestamp);
		return date == null ? null : date.toLocalDate();
	}
}
