package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SumOfNumbersProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("PrimeSum").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("in/prime_nums.text")
    val sumPrime = rdd.flatMap(row => {
      row.split("\\s+")
    }).filter(x => !x.isEmpty)
      .map(x => x.toInt)
      .reduce((x, y) => x + y)
    println(sumPrime)
  }
}
