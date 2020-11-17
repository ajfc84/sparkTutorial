package com.sparkTutorial.rdd.sumOfNumbers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SumOfNumbersProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */
        SparkConf conf = new SparkConf().setAppName("Prime_sum").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("in/prime_nums.text");
//        Integer sum = rdd
//                .map(line -> line.split("\t"))
//                .map(Integer::parseInt)
//                .reduce((x, y) -> x + y);
//        System.out.println(sum);
    }
}
