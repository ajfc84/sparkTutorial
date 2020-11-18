package com.sparkTutorial.pairRdd.filter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AirportsNotInUsaProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text;
           generate a pair RDD with airport name being the key and country name being the value.
           Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located,
           IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           ("Kamloops", "Canada")
           ("Wewak Intl", "Papua New Guinea")
           ...
         */
        SparkConf conf = new SparkConf().setAppName("USA_airport_finder").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("in/airports.text");
        JavaPairRDD<String, String> pairRDD = rdd
                .mapToPair(
                line -> {
                    String [] split = line.split(",");
                    return new Tuple2<>(split[1], split[3]);
                });
        pairRDD.filter(tuple2 -> !tuple2._2.equals("\"United States\""))
                .saveAsTextFile("out/airports_not_in_usa_pair.text");
    }
}
