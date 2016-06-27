package com.learnspark.secondarysort

import org.apache.spark.Partitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions

/*
 * An example of secondary sort in spark. Data is partitioned by sex field and records are presented to the reducer
 * sorted by age field.
 */
class SexPartitioner(partitions: Int) extends Partitioner {
  def numPartitions: Int = partitions;

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Person];
    return math.abs(k.sex.hashCode() % numPartitions);
  }
}
case class Person(sex: String, name: String, age: Int)

object Person {
  implicit def orderingByOmak[A <: Person] = new Ordering[A] {
    override def compare(x: A, y: A): Int = {
      -1 * x.age.compareTo(y.age)
    }
  }
}

object App {

  def main(args: Array[String]) {
    val config = new SparkConf;
    config.setMaster("local[4]");
    config.setAppName("Secondary Sort");
    val sc = new SparkContext(config);
    val numPartitions = 10;
    val lines = sc.textFile("/home/hany/data/*", numPartitions)

    val data = lines.map(r => 
      {
        val parts = r.split(",");
        (parts(0), parts(1).toInt, parts(2))
      })
      .map({ case (name, age, sex) => (new Person(sex, name, age), age) })
      .repartitionAndSortWithinPartitions(new SexPartitioner(numPartitions))
      .map({ case (person, age) => (person.sex, person.name, person.age) })
      .saveAsTextFile("/home/res-secondary-sort")
  }
}
