package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SameHostsProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
           Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

           Example output:
           vagrant.vf.mmc.com
           www-a1.proxy.aol.com
           .....

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */
        SparkConf conf = new SparkConf().setAppName("Both_Days_Finder").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd1 = sc.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> rdd2 = sc.textFile("in/nasa_19950801.tsv");
        JavaRDD<String> hostRdd1 = rdd1
                .filter(line -> !line.startsWith("host"))
                .map(line -> line.split("\t")[0]);
        JavaRDD<String> hostRdd2 = rdd2
                .filter(line -> !line.startsWith("host"))
                .map(line -> line.split("\t")[0]);
        hostRdd1.intersection(hostRdd2).saveAsTextFile("out/nasa_logs_same_hosts.csv");
    }
}
