package com.sparkTutorial.rdd.nasaApacheWebLogs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SameHostsProblem {

  def main(args: Array[String]) {

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
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("sameHost").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val rdd_0701 = sc.textFile("in/nasa_19950701.tsv").filter(x => isNotHeader(x))
    val rdd_0801 = sc.textFile("in/nasa_19950801.tsv").filter(x => isNotHeader(x))

    val host_0701 = rdd_0701.map(line => {
      line.split("\t")(0)
    })
    val host_0801 = rdd_0801.map(line => {
      line.split("\t")(0)
    })
    val same_host = host_0701.intersection(host_0801)
    same_host.saveAsTextFile("out/nasa_logs_same_hosts.csv")
  }
  def isNotHeader(line: String): Boolean = {
    !(line.startsWith("host") && line.endsWith("bytes"))
  }
}
