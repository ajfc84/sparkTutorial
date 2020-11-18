package com.sparkTutorial.pairRdd.groupbykey;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AirportsByCountryProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text,
           output the the list of the names of the airports located in each country.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           "Canada", ["Bagotville", "Montreal", "Coronation", ...]
           "Norway" : ["Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..]
           "Papua New Guinea",  ["Goroka", "Madang", ...]
           ...
         */
        SparkConf conf = new SparkConf().setAppName("GroupByKey").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("in/airports.text");
        JavaPairRDD<String, String> pairRDD = rdd.mapToPair(line -> {
            String [] split = line.split(Utils.COMMA_DELIMITER);
            return new Tuple2<>(split[3], split[2]);
        });
        pairRDD.groupByKey().saveAsTextFile("out/list_of_airports_by_country");
    }
}
