package de.fraunhofer.iais.kd.bda.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.HashSet;

public class SparkUserSet {
	public static void main(String[] args) {
	//	String inputFile= "/home/livlab/data/last-fm-sample1000000.tsv";
		
		String inputFile= "resources/last-fm-sample100000.tsv";
		String appName = "UserSet";
	
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
		
		JavaPairRDD<String, HashSet<String>> aggregatedUsers = artname2user.aggregateByKey(
				new HashSet<String>(),
				(set, element) -> {
					set.add(element);
					return set;
				},
				(set1, set2) -> {
					set1.addAll(set2);
					return set1;
				});

		 
		System.out.println(aggregatedUsers.count());
		aggregatedUsers.saveAsTextFile("resources/user_set_last_fm.txt");
		context.close();

	}
}
