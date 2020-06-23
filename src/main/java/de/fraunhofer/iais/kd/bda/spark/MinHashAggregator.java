package de.fraunhofer.iais.kd.bda.spark;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class MinHashAggregator implements Serializable{

	private static final long serialVersionUID = 1L;
	private long[] vals = new long[Basic.size()];
	
	public MinHashAggregator() {
		for(int i=0; i < Basic.size(); i++) {
			vals[i] = Long.MAX_VALUE;
		}
	}
	
	public MinHashAggregator add(String user) {
		for (int i = 0; i < Basic.size(); i++) {
			long hash = Basic.hash(i, user);
			if (hash < vals[i]) {
				vals[i] = hash;
			}
		}
		return this;
	}
	
	public MinHashAggregator combine(MinHashAggregator other) {
		for (int i = 0; i < Basic.size(); i++) {
			if (other.vals[i] < vals[i]) {
				vals[i] = other.vals[i];
			}
		}
		return this;
	}
	
	@Override
	public String toString() {
		final char SEP = ',';
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < Basic.size(); i++) {
			if (i > 0) {
				sb.append(SEP);
			}
			sb.append(vals[i]);
		}
		return sb.toString();
	}
	
	public double distanceTo(MinHashAggregator other) {
		double distance;
		int matches = 0;
		for (int i = 0; i < Basic.size(); i++) {
			if (other.vals[i] == vals[i]) {
				matches++;
			}
		}
		distance = 1 - (double) matches / (double) Basic.size();
		return distance;
	}
	
	public static void main(String[] args) {
		final String inputFile = "resources/last-fm-sample100000.tsv"; 
		final String appName = "MinHashAggragator";
		final String compareArtist = "Mark Knopfler"; 
		final double maxDistance = 0.65; 

		SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[*]"); 
		JavaSparkContext context = new JavaSparkContext(conf); 

		// Read file
		JavaRDD<String> input = context.textFile(inputFile);
		
		// just as in the UserSet program: 
		JavaPairRDD<String, MinHashAggregator> artistMinHashAggregator = input.mapToPair(line -> {
			String[] parts = line.split("\t"); 
			return new Tuple2<String, MinHashAggregator>(parts[3], (new MinHashAggregator()).add(parts[0].trim())); 
		}).aggregateByKey(new MinHashAggregator(), (a,b) -> a.combine(b), (a,b) -> a.combine(b))
		  .reduceByKey((a, b) -> a.combine(b)); 
		
		JavaPairRDD<String, String> artistMinHashs = artistMinHashAggregator.mapValues(t -> t.toString());
		
		artistMinHashs.saveAsTextFile("resources/artistMinHashs.txt");
		
		
		// We need the UserSet of the compareArtist to compute the Jaccard-Distance for all artists
		List<MinHashAggregator> comparisonMinHashAggregators = artistMinHashAggregator.lookup(compareArtist);
		
		// there should be exactly one tuple with compareArtist as key after the reduction 
		if(comparisonMinHashAggregators.size() != 1) {
			System.err.println(String.format("UserSet for %s not found!", compareArtist));
			System.exit(0);
		}
		
		final MinHashAggregator comparisonMinHashAggregator = comparisonMinHashAggregators.get(0); 
		
		System.out.println(comparisonMinHashAggregator);
		
		// compute the Jaccard-Distance of each artist to the obtained comparisonUserSet. 
		// The result contains of tuples (artname, jaccard distance)  
		JavaPairRDD<String, Double> artistDistance = artistMinHashAggregator.mapToPair(tuple -> {
			return new Tuple2<String, Double>(tuple._1, tuple._2.distanceTo(comparisonMinHashAggregator)); 
		}); 
		
		// filter out all entries having not the desired maxDistance 
		JavaPairRDD<String, Double> filteredArtistDistance = artistDistance.filter(tuple -> tuple._2.compareTo(maxDistance) <= 0); 
		
		filteredArtistDistance.saveAsTextFile("resources/closeToMark.txt");

		context.close();
		
		/* The Simple Minhashing has to communicate all the user sets of the artists between the nodes while the
		 * Minhash Aggregation has only to communicate the minhash value for the artist which is already computed
		 * for the values on each node.
		 */
		
		/* 
		 * $ grep -ir "Mark Knopfler" /tmp/artistMinHashs.txt 
		 * (Mark Knopfler,22,50,10,58,57,14,15,10,27,8,10,4,74,68,1,9,1,3,5,36)
		 */
	}
	
}


