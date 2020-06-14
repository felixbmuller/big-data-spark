package de.fraunhofer.iais.kd.bda.spark;

import java.util.HashSet;
import java.util.Arrays;

public class UserSet implements java.io.Serializable {
	
	private static final long serialVersionUID = 1071220102447838906L;
	private HashSet<String> userset;
	
	public UserSet() {
		
	    userset = new HashSet<String>();
	}
	
	public UserSet add(String username) {
		userset.add(username);
		
		return this;
	}
	
	public UserSet add(UserSet set) {
		for(String username : set.getUserHashSet()) {
			userset.add(username);
		}
		return this;
	}
	
	public HashSet<String> getUserHashSet(){
		return userset;
	}
	
	public double distanceTo(UserSet other) {
		double distance = 0;
		double intersection_size = 0;
		double union_size = 0;
		
		// Iterate over one set and count the users
		for(String username : userset) {
			if(other.getUserHashSet().contains(username)) {
				intersection_size++;
			}
			union_size++;
		}
		// Iterate over the other set and count the users, that were not counted before
		for(String username : other.getUserHashSet()) {
			if(!userset.contains(username)) {
				union_size++;
			}
			
		}
		
		if(union_size == 0)
			return 1.0; // two empty sets
			
		
		distance = 1.0 - intersection_size / union_size;
				
		return distance;
	}
	
	// This computes the column of the sketch matrix corresponding to one artist
	public String toMinHashSignature() {
		
		final int NUM_HASHFUNCTIONS = 20;
		
		String minHashes = "";
		long min, hash;
		for (int i = 0; i < NUM_HASHFUNCTIONS; i++) {
			
			min = Long.MAX_VALUE;
			for (String username : this.userset) {

				// Compute hash for all username, save minimum hash
				hash = Basic.hash(i, username);
				if (hash < min) {
					min = hash;
				}
			}
			
			minHashes += min + (i < 19 ? ", " : ""); // Add minimum hash to return string
		}

		return minHashes;
	}
	
	
	public static void main(String[] args) {
		UserSet userset_1 = new UserSet();
		userset_1.add("user");
		userset_1.add("user2");
		
		UserSet userset_2 = new UserSet();
		userset_2.add("user");
		userset_2.add("user3");
		
		double distance = userset_1.distanceTo(userset_2);
		
		System.out.println(distance);
		
	}
	
	
}
