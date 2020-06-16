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
		
		// This is something I personally have to set such that spark works for me
		//System.setProperty("hadoop.home.dir", "path_to\\winutil");
		
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
		
		
		/* Task 4 sheet 6 */
		
		inputFile = "resources/user_set_last_fm.txt";
		appName = "UserMinHash";

		conf = new SparkConf().setAppName(appName).setMaster("local[*]");
		context = new JavaSparkContext(conf);
		
		input = context.textFile(inputFile);
		
		JavaPairRDD<String, UserSet> art2user = input.mapToPair(line -> {
			String[] parts = line.split(",");
			String[] users = parts[1].split(",");
			
			UserSet us = new UserSet();
			for (String user : users)
				us.add(user.trim());
			
			return new Tuple2<String, UserSet>(parts[0], us);
		});
		
		JavaPairRDD<String, String> art2minhash = art2user.mapToPair(artistUsersetPair -> {
			return new Tuple2<String, String>(artistUsersetPair._1, artistUsersetPair._2.toMinHashSignature());
		});
		
		// Minhash of Mark Knopfler: 665, 967, 554, 143, 326, 922, 90, 686, 863, 625, 222, 993, 165, 759, 944, 704, 468, 57, 828, 0
		art2minhash.saveAsTextFile("resources/artist_to_minhashes.txt");
		context.close();
		
		
	
	}
	
	/**
	 * 
	 * According to grep, this is the user set of Mark Knopfler, which matches with the results of the above algorithm.
	 * User 680 was not found by the Spark algorithm, because Mark Knopfler & Emmylou Harris is not the same as Mark Knopfler.
user_000603	2008-11-18T18:45:12Z	e49f69da-17d5-4c5c-bac0-dadcb0e588f5	Mark Knopfler		Brothers In Arms (Live At Abbey Road)
user_000095	2008-06-09T21:31:41Z	e49f69da-17d5-4c5c-bac0-dadcb0e588f5	Mark Knopfler	ac84a7c8-5392-4d65-a6ef-760d70c8323b	The Long Road
user_000680	2007-10-02T01:22:29Z	9541a456-6c13-401a-932c-bcf76e28840f	Mark Knopfler & Emmylou Harris	742dc324-0f6c-44ae-ae78-ca0edeb6a995	Donkey Town
user_000095	2008-09-20T02:27:22Z	e49f69da-17d5-4c5c-bac0-dadcb0e588f5	Mark Knopfler	e45079c0-f563-4d61-a6d5-13000351613f	Once Upon A Time... Storybook Love
user_000348	2006-01-29T17:48:21Z	e49f69da-17d5-4c5c-bac0-dadcb0e588f5	Mark Knopfler	e2b7045f-5d8c-4af3-b8e3-da037777a007	Quality Shoe
user_000934	2008-12-08T15:23:33Z	e49f69da-17d5-4c5c-bac0-dadcb0e588f5	Mark Knopfler	437bb215-413a-418a-8039-a409e0e48eb0	What It Is
user_000906	2009-02-10T20:02:33Z	e49f69da-17d5-4c5c-bac0-dadcb0e588f5	Mark Knopfler	437bb215-413a-418a-8039-a409e0e48eb0	What It Is
user_000095	2007-07-31T14:32:43Z	e49f69da-17d5-4c5c-bac0-dadcb0e588f5	Mark Knopfler	437bb215-413a-418a-8039-a409e0e48eb0	What It Is
user_000934	2008-04-08T20:33:43Z	e49f69da-17d5-4c5c-bac0-dadcb0e588f5	Mark Knopfler	4aed71cc-cf66-4111-b6d7-e5f91018490e	True Love Will Never Fade
user_000348	2008-11-23T10:14:39Z	e49f69da-17d5-4c5c-bac0-dadcb0e588f5	Mark Knopfler	9148755c-c63c-4fac-a123-af0021ae08d2	Who'S Your Baby Now
user_000002	2006-11-27T06:40:38Z	e49f69da-17d5-4c5c-bac0-dadcb0e588f5	Mark Knopfler		Sailing To Philadelphia (Featuring James Taylor)
user_000348	2008-11-23T10:09:09Z	e49f69da-17d5-4c5c-bac0-dadcb0e588f5	Mark Knopfler	60a258b1-3794-4e42-8131-9ebd29066411	Sailing To Philadelphia
	 */
}
