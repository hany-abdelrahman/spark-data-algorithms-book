package com.spark.experiments

import scala.collection.Iterator
import scala.collection.mutable.PriorityQueue
import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class Person(sex: String, name: String, age: Int) extends Serializable {
  override def hashCode(): Int = {
    this.age.hashCode()
  }
    
  override def equals(other: Any) = other match {
    case that: Person => this.age.equals(that.age);
    case _ => false
  }
}

object App {

  def main(args: Array[String]) {
    val config = new SparkConf;
    config.setMaster("local[4]");
    config.setAppName("test");
    config.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(config);
    val numPartitions = 10;
    val K = 10;
    val lines = sc.textFile("/home/hany/data/*", numPartitions)
		val orderingStrategy =  (Ordering.by[Person,Integer]( b => b.age)).reverse
    val data = lines.map(r => {
      val parts = r.split(",");
      (parts(0), parts(1).toInt, parts(2))
      })
      .map({ case (name, age, sex) => new Person(sex, name, age) });
    
    def topNInPartition(N : Int, iterator : Iterator[Person], heap : PriorityQueue[Person]) : PriorityQueue[Person] = {
      val ordering = new com.google.common.collect.Ordering[Person] {
            override def compare(l: Person, r: Person): Int = orderingStrategy.compare(l, r);
          }
      val result = new PriorityQueue[Person]()(orderingStrategy)
      heap.++(ordering.leastOf(iterator.asJava, N).asScala);
    }
    
    def mergeHeaps(m1 : PriorityQueue[Person], m2 : PriorityQueue[Person]) : PriorityQueue[Person] = {
      var result = new PriorityQueue[Person]()(orderingStrategy)
      result = topNInPartition(K, m1.iterator, result);
      result = topNInPartition(K, m2.iterator, result);      
      result
    }
    
    data
      .mapPartitions(f => {
        val result = new PriorityQueue[Person]()(orderingStrategy)
        Seq(topNInPartition(K, f, result)).iterator
        }, true)
      .reduce((m1,m2) => mergeHeaps(m1, m2))
      .foreach(println)
  }
}
