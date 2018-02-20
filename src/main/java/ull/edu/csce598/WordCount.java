package ull.edu.csce598;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class WordCount implements Serializable{
	private static final Logger LOGGER = LoggerFactory.getLogger(WordCount.class);
	
	public void run(String inputFilePath) {
	    String master = "local[*]";

	    SparkConf conf = new SparkConf()
	        .setAppName(WordCount.class.getName())
	        .setMaster(master);
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    JavaRDD<String> textFile = sc.textFile(inputFilePath);
	    
	    JavaPairRDD<String, Integer> counts = textFile
	    	    .flatMap(new FlatMapFunction<String, String>() {
	    	        @Override public Iterable<String> call(String x) {
	    	            return Arrays.asList(x.split(" "));
	    	          }
	    	        })
	    	    .mapToPair( new PairFunction<String, String, Integer>() {
	    	        @Override public Tuple2<String, Integer> call(String s) {
	    	            return new Tuple2<String, Integer>(s, 1);
	    	          }
	    	        })
	    	    .reduceByKey(new Function2<Integer, Integer, Integer>() {
	    	        @Override public Integer call(Integer i1, Integer i2) {
	    	            return i1 + i2;
	    	          }
	    	        });
	    
	    counts.foreach(result -> LOGGER.info(
	            String.format("Word [%s] count [%d].", result._1(), result._2)));
	    sc.close();
	
	  }
	  


}
