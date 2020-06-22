package de.fraunhofer.iais.kd.bda.spark;

import java.io.Serializable;

public class MinHashAggregator implements Serializable {
	
	public static void main(String[] args) {
		
		MinHashAggregator agg2 = new MinHashAggregator();
		
		agg2.add("User 3");
		
		System.out.println(Basic.hash(0, "User 3"));
		
		long a,b, x,p,r ;
	      x = "User 3".hashCode();
	      a = Basic.getPrime(0+1);
	      b = Basic.getPrime(0+1+20);
	      p = 1009;
	      r = 1000;

	      long hash = (((a*x + b) % p ) %r );
	    
	      System.out.println(a);
	      System.out.println(a);
	      System.out.println(b);
	      System.out.println(p);
	      System.out.println(r);
	      System.out.println(hash);
		
		System.out.println(agg2.toString());
		
		MinHashAggregator agg = new MinHashAggregator();
		
		System.out.println(agg.toString());
		
		agg.add("User1");
		
		System.out.println(agg.toString());
		
		agg.add("User2");
		
		System.out.println(agg.toString());
		
		
		
		agg.merge(agg2);
		
		System.out.println(agg.toString());
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 18767658L;
	private long[] minHashes;
	
	public MinHashAggregator() {
		minHashes = new long[20];
		for(int i = 0; i < 20; i++) {
			minHashes[i] = Long.MAX_VALUE;
		}
	}
			
	public MinHashAggregator add(String user) {
		for(int i = 0; i < 20; i++) {
			 long hashi = Basic.hash(i, user);
			 minHashes[i] = Math.min(hashi, minHashes[i]);
		}
		return this;
	}
	
	public MinHashAggregator merge(MinHashAggregator other) {
		for(int i = 0; i < 20; i++) {
			minHashes[i] = Math.min(minHashes[i], other.minHashes[i]);
		}
		return this;
	}
	
	public String toString() {
		
		StringBuilder sb = new StringBuilder();
		
		for(int i = 0; i < 20; i++) {
			sb.append(minHashes[i]);
			if(i < 19) {
				sb.append(", ");
			}
		}
		
		return sb.toString();
	}
}
