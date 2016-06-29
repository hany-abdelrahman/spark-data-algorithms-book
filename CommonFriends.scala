package com.spark.experiments

import scala.collection.Iterator
import scala.collection.JavaConverters._
import scala.collection.immutable.HashSet

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.HashMap

object App {

  def main(args: Array[String]) {
    val config = new SparkConf;
    config.setMaster("local[4]");
    config.setAppName("test");
    config.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(config);
    val numPartitions = 5;
    val lines = sc.textFile("/home/hany/data/graph.txt", numPartitions)
    val data = lines.map(l => {
      val vals = new HashMap[scala.collection.mutable.Set[String], Set[String]]
      val parts = l.split(",");      
      val userId = parts(0);
      val friends = parts(1);
      var s = new HashSet[String]
      s = s.++(friends.split(" "));
      s.foreach(elem => {
        val v = s.-(elem);
        val k = new scala.collection.mutable.HashSet[String];
        k.+=(elem)
        k.+=(userId)
        vals.+=((k,v))
      });
      vals
      })
    .flatMap(r => r)
    .reduceByKey((a,b) => a.intersect(b))
    .foreach(println)
  }
}
