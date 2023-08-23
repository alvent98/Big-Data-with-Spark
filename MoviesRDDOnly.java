package org.myorg;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class MoviesRDDOnly {

	private static final Pattern DOUBLECOLON = Pattern.compile("::");
	//WARNING: The path provided has to be changed, according to your specific path
	private static final String path = "C:\\Alexandros\\PostGrad\\Big_Data\\Exercise\\";

	public static void main(String[] args) throws Exception {

		//All the classes that are used in the main class (auxiliaries; used as arguments of mapToPairs to augment code readability)
		//The class responsible for the parsing of the ratings.dat file
		class SplitRatings implements PairFunction<String,Integer,Float> {
			private static final long serialVersionUID = 1L; //Added in order to remove a warning; useless otherwise

			public Tuple2<Integer,Float> call(String ratingTuple) {
				return new Tuple2<Integer,Float> (Integer.parseInt(DOUBLECOLON.split(ratingTuple)[1]),Float.parseFloat(DOUBLECOLON.split(ratingTuple)[2]));
			}
		}

		//The class responsible for the parsing of the movies.dat file
		class SplitMovies implements PairFunction<String,Integer,String> {
			private static final long serialVersionUID = 1L; //Added in order to remove a warning; useless otherwise

			public Tuple2<Integer,String> call(String movieTuple) {
				return new Tuple2<Integer,String> (Integer.parseInt(DOUBLECOLON.split(movieTuple)[0]),DOUBLECOLON.split(movieTuple)[1]);
			}
		}   

		//The class responsible for calculating the average of each movie, from the subset of movies with a December rating
		class CalculateAverage implements PairFunction<Tuple2<Integer, Iterable<Float>>, Integer, Float> {
			private static final long serialVersionUID = 1L; //Added in order to remove a warning; useless otherwise

			public Tuple2<Integer,Float> call(Tuple2<Integer, Iterable<Float>> tuple) {
				List<Float> t_2TransformedToList = new ArrayList<Float>();
				//Convert Iterable<String> to ArrayList<String>
				tuple._2().forEach(t_2TransformedToList::add);

				//Calculate average
				float sum = 0;
				for(Float rating : t_2TransformedToList) {
					sum += rating;
				}
				float average = sum / t_2TransformedToList.size();

				return new Tuple2<Integer,Float>(tuple._1() ,average);    	
			}
		}

		//The class responsible for converting Tuple2<String,Tuple2<String,Float>> to 	
		class convertToSimplePair implements PairFunction<Tuple2<Integer, Tuple2<String,Float>>, String, Float> {
			private static final long serialVersionUID = 1L; //Added in order to remove a warning; useless otherwise

			public Tuple2<String,Float> call( Tuple2<Integer, Tuple2<String,Float>> s) {
				return new Tuple2<String,Float> (s._2()._1(), s._2()._2());
			}
		}   

		SparkConf sparkConf = new SparkConf().setAppName("MoviesRDDOnly");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> inputRatingsRDD = sc.textFile(path+"input\\ratings.dat");
		JavaRDD<String> inputMoviesRDD = sc.textFile(path+"input\\movies.dat");


		//A) Find 25 most rated movies	
		//Create the JavaPairRDD with the <movieId,rating> tuples
		JavaPairRDD<Integer,Float> movieIdMovieRatingPairRDD = inputRatingsRDD.mapToPair(new SplitRatings());       

		//Create the JavaPairRDD with the <movieId,movieTitle> tuples
		JavaPairRDD<Integer,String> movieIdMovieTitlePairRDD = inputMoviesRDD.mapToPair(new SplitMovies());

		//Create the JavaPairRDD with the <movieId,#ofAppearances> tuples
		JavaPairRDD<Integer,Integer> movieIdNumberOfAppearancesPairRDD = movieIdMovieRatingPairRDD.keys()
				.mapToPair(t -> new Tuple2<Integer,Integer> (t, 1))
				.reduceByKey((x, y) -> (int) x + (int) y);        

		//In order to sort the PairRDD, we have to swap the <key,value> pairs
		JavaPairRDD<Integer,Integer> NumberOfAppearancesmovieIdPairRDD = 
				movieIdNumberOfAppearancesPairRDD.mapToPair(t -> new Tuple2<Integer,Integer> (t._2, t._1));

		//Sort the swapped PairRDD
		NumberOfAppearancesmovieIdPairRDD = NumberOfAppearancesmovieIdPairRDD.sortByKey(false); //ascending == false
		//        NumberOfAppearancesmovieIdPairRDD.take(25).forEach(s -> System.out.println(s));

		//Take the first 25 element of the RDD and store them to a List (no way to store them directly to another PairRDD)
		List<Tuple2<Integer,Integer>> listNumberOfAppearancesmovieId = NumberOfAppearancesmovieIdPairRDD.take(25);

		//ReInitialize the PairRDD with the contents of the list
		NumberOfAppearancesmovieIdPairRDD = sc.parallelizePairs(listNumberOfAppearancesmovieId);

		//Swap again the sorted PairRDD
		movieIdNumberOfAppearancesPairRDD = NumberOfAppearancesmovieIdPairRDD
				.mapToPair(t -> new Tuple2<Integer,Integer> (t._2, t._1));
		//        movieIdNumberOfAppearancesPairRDD.foreach(s -> System.out.println(s));

		JavaPairRDD<Integer, Tuple2<Integer,String>> the25MostRatedMovieIdsWithMovieTitlesPairRDD =
				movieIdNumberOfAppearancesPairRDD.join(movieIdMovieTitlePairRDD);

		System.out.println("The 25 most rated movies are:");
		FileWriter fw1 = new FileWriter(path+"output\\mostRatedMovies.txt");    
		the25MostRatedMovieIdsWithMovieTitlesPairRDD.take(25).forEach(tuple -> {
			System.out.println(tuple);
			try {
				fw1.write(tuple.toString()+"\n");
			} catch (IOException e) {
				// Auto-generated catch block
				e.printStackTrace();
			}    
		});
		fw1.close();

		//B) Find the comedies rated >= 3
		//Create the RDD that contains only the lines with the comedies 
		JavaRDD<String> comedyLinesRDD = inputMoviesRDD.filter(tuple -> tuple.contains("Comedy"));

		//Create the PairRDD that contains only the tuples <movieId, movieTitle> of the comedies
		JavaPairRDD<Integer,String> comedyIdComedyTitlePairRDD = comedyLinesRDD.mapToPair(new SplitMovies());

		//Create the PairRDD that contains only the tuples <movieId, movieRating> of the comedies rated >= 3
		JavaPairRDD<Integer,Float> movieIdMovieRatingGTE3PairRDD = movieIdMovieRatingPairRDD.filter(tuple -> tuple._2() >= 3);

		//Create the PairRDD with the join result of the previous two PairRDD's
		JavaPairRDD<Integer, Tuple2<String,Float>> joinedPairRDDs = comedyIdComedyTitlePairRDD.join(movieIdMovieRatingGTE3PairRDD);

		//Count the number of unique keys in join result
		int numberOfComediesRatedGTE3 = joinedPairRDDs.countByKey().size();

		//Save and print the results
		System.out.println(numberOfComediesRatedGTE3+" comedies were rated with 3 or more points."); 
		FileWriter fw2 = new FileWriter(path+"output\\comedies.txt");   
		fw2.write(numberOfComediesRatedGTE3+" ");
		fw2.write("comedies were rated with 3 or more points.");
		fw2.close();


		//C) Find the top 10 romance movies for the ratings of the Decembers
		//Create the RDD that contains only the lines with the romances
		JavaRDD<String> romanceLinesRDD = inputMoviesRDD.filter(tuple -> tuple.contains("Romance"));

		//Create the PairRDD that contains only the tuples <movieId, movieTitle> of the romances
		JavaPairRDD<Integer,String> romanceIdRomanceTitlePairRDD = romanceLinesRDD.mapToPair(new SplitMovies());

		//Filter the RDD with the lines, so that it contains only the ratings made at any December
		JavaRDD<String> decemberLinesRDD = inputRatingsRDD.filter(tuple -> {
			String ratingTime = DOUBLECOLON.split(tuple)[3];
			LocalDate ratingDate = getDateFromTimestamp(Long.parseLong(ratingTime));
			return (ratingDate.getMonthValue() == 12);
		});

		//Create the PairRDD that contains only the tuples <movieId, movieRating> from the subset of movies with a December rating
		JavaPairRDD<Integer,Float> movieIdRating12PairRDD = decemberLinesRDD.mapToPair(new SplitRatings());

		//Create the PairRDD with the ratings, grouped by movieId, with format <movieId, <[rating1, rating2, ..., ratingN]>>
		JavaPairRDD<Integer, Iterable<Float>> movieIdRatingPairRDDGroupedById = movieIdRating12PairRDD.groupByKey();             

		//Create the JavaPairRDD with the <movieId,movieAverageRating> tuples, from the subset of movies with a December rating
		JavaPairRDD<Integer,Float> averageMovieIdDecemberRatingPairRDD = movieIdRatingPairRDDGroupedById.mapToPair(new CalculateAverage());

		//Create the PairRDD with the join result of the previous two PairRDD's
		JavaPairRDD<Integer, Tuple2<String,Float>> romanceDecemberjoined = 
				romanceIdRomanceTitlePairRDD.join(averageMovieIdDecemberRatingPairRDD);

		//Keep only the useful <romanceId, romancdRating> from the join result
		JavaPairRDD<String,Float> romanceIdRomanceRatingPairRDD = romanceDecemberjoined.mapToPair(new convertToSimplePair());

		//In order to sort the PairRDD, we have to swap the <key,value> pairs
		JavaPairRDD<Float,String> romanceRatingRomanceIdPairRDD = 
				romanceIdRomanceRatingPairRDD.mapToPair(t -> new Tuple2<Float,String> (t._2, t._1));

		//Sort the swapped PairRDD
		romanceRatingRomanceIdPairRDD = romanceRatingRomanceIdPairRDD.sortByKey(false); //ascending == false

		//Take the first 10 element of the RDD and store them to a List (no way to store them directly to another PairRDD)
		List<Tuple2<Float,String>> listRomanceRatingRomanceId = romanceRatingRomanceIdPairRDD.take(10);

		//ReInitialize the PairRDD with the contents of the list
		romanceRatingRomanceIdPairRDD = sc.parallelizePairs(listRomanceRatingRomanceId);

		//Swap again the sorted PairRDD
		romanceIdRomanceRatingPairRDD = romanceRatingRomanceIdPairRDD
				.mapToPair(t -> new Tuple2<String,Float> (t._2, t._1));

		FileWriter fw3 = new FileWriter(path+"output\\romances.txt"); 
		System.out.println("The 10 most rated romances, rated at any December, are: ");		

		romanceIdRomanceRatingPairRDD.take(10).forEach(tuple -> {
			System.out.println(tuple);        	  
			try {
				fw3.write(tuple+"\n");
			} catch (IOException e) {
				// Auto-generated catch block
				e.printStackTrace();
			}
		}); 

		fw3.close();
		sc.close();
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

//Manually select main class in jar: https://stackoverflow.com/questions/12811618/eclipse-manually-select-main-class-for-executable-jar-file
//How to sort a map<key,value> by values: https://stackoverflow.com/questions/109383/sort-a-mapkey-value-by-values
//Class-oriented implementation of document parsing: https://spark.apache.org/docs/0.8.1/java-programming-guide.html
//MapToPair & reduceByKey combination: https://www.journaldev.com/20342/apache-spark-example-word-count-program-java
//Sort by value in PairRDD: https://stackoverflow.com/questions/29003246/how-to-achieve-sort-by-value-in-spark-java
//Key-value swapping in pairRDD: https://blog.ippon.tech/apache-spark-mapreduce-rdd-manipulations-keys/
//Timestamp to LocalDate: http://www.java2s.com/Tutorials/Java/Data_Type_How_to/Date_Convert/Convert_long_type_timestamp_to_LocalDate_and_LocalDateTime.htm
//Convert Iterable to Collection: https://www.baeldung.com/java-iterable-to-collection