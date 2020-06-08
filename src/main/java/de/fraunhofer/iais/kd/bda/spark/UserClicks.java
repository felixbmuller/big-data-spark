package de.fraunhofer.iais.kd.bda.spark;


import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;


public class UserClicks {
	public static void main(String[] args) {
		
		// This is something I personally have to set such that spark works for me
		//System.setProperty("hadoop.home.dir", "PATH_TO_WINUTIL\\winutil");

		String inputFile= "resources/last-fm-sample100000.tsv";
		String appName = "UserClicks";
	
		SparkConf conf  = new SparkConf().setAppName(appName).setMaster("local[*]");		
		JavaSparkContext context = new JavaSparkContext(conf);
		
		//Read file
		JavaRDD<String> input = context.textFile(inputFile);
		
		//Map line to artist
		JavaRDD<String> artists = input.map(line ->
		{
			// Artists are always the third tab in the file
			String artist = line.split("\t")[3];
			return artist;
		});
		
		// Append a "1" to each artist
		JavaPairRDD<String, Integer> artistsOne = artists.mapToPair(artist -> {return new Tuple2<String,Integer>(artist,new Integer(1));});
		
		// Count occurrences of each artist
		JavaPairRDD<String, Integer> artistCount = artistsOne.reduceByKey((a, b) -> a + b);

		// This file contains all the (artist, count) pairs
		artistCount.saveAsTextFile("resources/artistcount.txt");

		// This should print 11 (Occurences of Mark Knopfler alone)
		// Inspecting the file there is also 1 additional entry: (Mark Knopfler & Emmylou Harris, 1)
		// So in total he appears 12 times
		System.out.println("Count of Mark Knopfler is: " + artistCount.lookup("Mark Knopfler").toString());
		
		context.close();

	}
}
