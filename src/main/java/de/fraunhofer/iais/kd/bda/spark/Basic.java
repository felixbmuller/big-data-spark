package de.fraunhofer.iais.kd.bda.spark;

import java.util.*;
import java.lang.*;
import java.io.*;

class Basic
{


  public static long hash(int i, String value)
  {
      long a,b, x,p,r ;
      x = value.hashCode();
      a = getPrime(i+1);
      b = getPrime(i+1+20);
      p = 1009;
      r = 1000;

      long hash = (((a*x + b) % p ) %r );
      return hash;
  }

  static long getPrime(int value){

      int max = value +1 , counter = 1;
      long primeValues[] = new long[max];
      primeValues[0] = 2;
      int n=2;
      while(counter < max){
          if(isPrime(n))
              primeValues[counter++] = n;
          n++;

      }
      value--;
      return primeValues[value];


  }
  static boolean isPrime(int n)
  {
      if (n%2==0) return false;
  	//if not, then just check the odds
  	for(int i=3;i*i<=n;i+=2) {
      if(n%i==0)
          return false;
  	}
  	return true;
  }
  
  static int size() {
	  return 20;
  }


}
