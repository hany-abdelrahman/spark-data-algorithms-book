package com.spark.Experiments

import scala.collection.Iterator
import scala.collection.mutable.PriorityQueue
import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import org.apache.spark.Partitioner
import org.apache.spark.Aggregator
import scala.collection.mutable.Queue

class StockPartitioner(partitions : Int) extends Partitioner {
  def numPartitions : Int = partitions; 
  
  def getPartition(key : Any) : Int = key match {
    case null => 0
    case _ => {

    	val stockKey : StockKey = key.asInstanceOf[StockKey];
      val rawMod = stockKey.company.hashCode() % numPartitions
      rawMod + (if(rawMod < 0)  numPartitions else 0)
    }
  } 
}

object StockKey {
  implicit def orderingByDate[A <: StockKey] : Ordering[A] = new Ordering[A] {
    override def compare(x: A, y: A): Int = {
      if(x.company != y.company)
        x.company.compareTo(y.company)
      else 
        x.date.compareTo(y.date)
    }
  }
}

case class StockKey(company: String, date: Long) {
}

object App {

  def main(args: Array[String]) {
    val config = new SparkConf;
    config.setMaster("local[4]");
    config.setAppName("test");
    config.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(config);
    val numPartitions = 1;
    val K = 10;
    val lines = sc.textFile("/home/hany/data/stocks.txt", numPartitions)
	  val data = lines.map(r => { 
      val parts = r.split(",");
      val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
      val date : Long = format.parse(parts(1)).getTime

      (new StockKey(parts(0), date), parts(2).toDouble)
    })
    .repartitionAndSortWithinPartitions(new StockPartitioner(numPartitions))
    .mapPartitions(f => {
       val windowSize : Int = 2;
       val result = new ExternalAppendOnlyMap[StockKey, Double, Double]((v: Double) => v, _ + _, _ + _);
       val window = new Queue[Double];
       val lastKey : Option[StockKey] = None; 
       var sum = 0.0;
       while(f.hasNext) {
            val (key,value) = f.next();
            if (lastKey.isDefined && key.company != lastKey.get.company) {
              window.dequeueAll(x => true);
              sum = 0;
            }
            window.enqueue(value);
            sum += value;
            if (window.size > windowSize) {
            	val removedVal = window.dequeue()
            	sum = sum - removedVal;                  
            }
            val mean = sum / window.size;
            result.insert(key, mean);
       }
       result.iterator
    }, true)
    .foreach(println)
  }
}
