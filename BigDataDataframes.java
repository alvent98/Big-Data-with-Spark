package org.myorg;
import java.io.FileWriter;
import java.io.IOException;
//
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.desc;


public class BigDataDataframes {
	private static final Pattern DOUBLECOLON = Pattern.compile("::");
	//WARNING: The path provided has to be changed, according to your specific path
	private static final String path = "C:\\Alexandros\\PostGrad\\Big_Data\\Exercise\\";
	
	public static void main(String[] args) {
		
		//All the classes that are used in the main class (auxiliaries; used as arguments of mapToPairs to augment code readability)
		//The class responsible for the parsing of the ratings.dat file
		class ParseRatings implements Function<String,Rating> {
			private static final long serialVersionUID = 1L; //Added in order to remove a warning; useless otherwise

			public Rating call(String ratingTuple) {				
				return new Rating(Integer.parseInt(DOUBLECOLON.split(ratingTuple)[1]),
									Float.parseFloat(DOUBLECOLON.split(ratingTuple)[2]),
									Rating.calculateMonth(DOUBLECOLON.split(ratingTuple)[3]));
			}
		}
		
		//The class responsible for the parsing of the movies.dat file
		class ParseMovies implements Function<String,Movie> {
			private static final long serialVersionUID = 1L; //Added in order to remove a warning; useless otherwise

			public Movie call(String movieTuple) {				
				return new Movie(Integer.parseInt(DOUBLECOLON.split(movieTuple)[0]),
									DOUBLECOLON.split(movieTuple)[1],
									DOUBLECOLON.split(movieTuple)[2]);
			}
		}		
		
		SparkConf sparkConf = new SparkConf().setAppName("MoviesRDDOnly");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		SparkSession sparkSession = SparkSession.builder().config(sc.getConf()).getOrCreate();
		
		JavaRDD<String> inputRatingsRDD = sc.textFile(path+"input\\ratings.dat");
		JavaRDD<String> inputMoviesRDD = sc.textFile(path+"input\\movies.dat");
		
		//Create the JavaRDDs with the Rating and Movie tuples
		JavaRDD<Rating> ratingsRDD = inputRatingsRDD.map(new ParseRatings());
		JavaRDD<Movie> moviesRDD = inputMoviesRDD.map(new ParseMovies());
		
		//Create the Datasets for each class
		Dataset<Row> ratingsDataset = sparkSession.createDataFrame(ratingsRDD, Rating.class);
		Dataset<Row> moviesDataset = sparkSession.createDataFrame(moviesRDD, Movie.class);
		
		//a) Find 25 most rated movies
		Dataset<Row> ratingsSubdataset = ratingsDataset.groupBy("movieId").count();
		ratingsSubdataset = ratingsSubdataset.sort(desc("count")).limit(25);
		
		Dataset<Row> subquestionResult = ratingsSubdataset.
				join(moviesDataset, moviesDataset.col("movieId").equalTo(ratingsSubdataset.col("movieId")));
	
		System.out.println("The 25 most rated movies are:");
		subquestionResult.show();	  
		subquestionResult.select("movieTitle").write().text(path+"output2\\mostRatedMovies");
		
		//B) Find the comedies rated >= 3
		ratingsSubdataset = ratingsDataset.groupBy("movieId").max("movieRating");
		ratingsSubdataset = ratingsSubdataset.filter(ratingsSubdataset.col("max(movieRating)").$greater$eq(3));
		
		Dataset<Row> moviesSubdataset = moviesDataset.filter(moviesDataset.col("genres").contains("Comedy"));
		
		subquestionResult = moviesSubdataset.
				join(ratingsSubdataset, ratingsSubdataset.col("movieId").equalTo(moviesSubdataset.col("movieId")));
		
		long numberOfComedies = subquestionResult.count();
		
		System.out.println(numberOfComedies+" comedies were rated with 3 or more points.");
		
		try {
			FileWriter fw2 = new FileWriter(path+"output2\\comedies.txt");   
			fw2.write(numberOfComedies+" comedies were rated with 3 or more points.");
			fw2.close();
		} catch (IOException e) {
			// Auto-generated catch block
			e.printStackTrace();
		}
		
		
		//C) Find the top 10 romance movies for the ratings of the Decembers
		ratingsSubdataset = ratingsDataset.filter(ratingsDataset.col("month").equalTo(12));		
		ratingsSubdataset = ratingsSubdataset.groupBy("movieId").avg("movieRating");
		
		moviesSubdataset = moviesDataset.filter(moviesDataset.col("genres").contains("Romance"));
		
		subquestionResult = moviesSubdataset.
				join(ratingsSubdataset, ratingsSubdataset.col("movieId").equalTo(moviesSubdataset.col("movieId")));
		
		subquestionResult = subquestionResult.sort(desc("avg(movieRating)"));
		
		System.out.println("The 10 most highly rated romance movies for any December are:");
		System.out.println(subquestionResult.head(10));  
		subquestionResult.limit(10).select("movieTitle").write().text(path+"output2\\romances");
		
		
		//D) Find the most rated movies of the Decembers
		ratingsSubdataset = ratingsDataset.filter(ratingsDataset.col("month").equalTo(12));		
		ratingsSubdataset = ratingsSubdataset.groupBy("movieId").count();
		ratingsSubdataset = ratingsSubdataset.sort(desc("count")).limit(10);
		
		subquestionResult = moviesDataset.
				join(ratingsSubdataset, ratingsSubdataset.col("movieId").equalTo(moviesDataset.col("movieId")));
		
		System.out.println("The 10 most rated movies for any December are:");
		System.out.println(subquestionResult.head(10));
		subquestionResult.limit(10).select("movieTitle").write().text("C:\\Alexandros\\PostGrad\\Big_Data\\Exercise\\output2\\mostRatedDecem");
		
		sc.close();				
	}	
}
//for the sparksession creation: https://www.tabnine.com/code/java/classes/org.apache.spark.sql.SparkSession
//Group by count: https://sparkbyexamples.com/spark/using-groupby-on-dataframe/
//Order by desc: https://stackoverflow.com/questions/44114642/how-to-order-by-desc-in-apache-spark-dataset-using-java-api
