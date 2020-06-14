package de.fraunhofer.iais.kd.bda.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class CloseToMark {
	public static void main(String[] args) {
	//	String inputFile= "/home/livlab/data/last-fm-sample1000000.tsv";
		
		String inputFile= "resources/last-fm-sample100000.tsv";
		String appName = "CloseToMark";
	
		SparkConf conf  = new SparkConf().setAppName(appName)
										 .setMaster("local[*]");
		
		JavaSparkContext context = new JavaSparkContext(conf);
		
		//Read file
		JavaRDD<String> input = context.textFile(inputFile);
		
		
		JavaPairRDD<String, String> artname2user = input.mapToPair(line -> {
			String[] parts = line.split("\t");
			// return artist-user-pairs
			return new Tuple2<String,String>(parts[3],parts[0]);
		});
		
		JavaPairRDD<String, UserSet> aggregatedUsers = artname2user.aggregateByKey(
				new UserSet(),
				(set, element) -> {
					set.add(element);
					return set;
				},
				(set1, set2) -> {
					set1.add(set2);
					return set1;
				});
		

		// get userset of Mark Knopfler
		UserSet usersetMarkKnopfler = aggregatedUsers.filter(tuple -> tuple._1.equals("Mark Knopfler")).first()._2;
		
		// jaccard distance to Mark Knopfler for each artist		
		JavaPairRDD<String, Double> artist2jaccardDistance = aggregatedUsers.mapToPair( 
				tuple -> new Tuple2<String, Double>(tuple._1, tuple._2.distanceTo(usersetMarkKnopfler))
		);
		
		JavaPairRDD<String, Double> relevantArtists = artist2jaccardDistance.filter(name2dist -> name2dist._2 < 0.85);
		 
		System.out.println(relevantArtists.count());
		relevantArtists.saveAsTextFile("resources/close_to_mark_artists.txt");
		context.close();

		/**
		 * The distance between Mark Knopfler and himself is 0.0, just as expected.
		 */
	}
	
}
