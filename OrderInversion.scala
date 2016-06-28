package com.spark.experiments

import scala.collection.Iterator
import scala.collection.mutable.PriorityQueue
import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import org.apache.spark.Partitioner
import scala.collection.mutable.ArrayBuffer


class BigramPartitioner(partitions : Int) extends Partitioner {
   def numPartitions : Int = partitions;
   
   def getPartition(key: Any) : Int = {
     val bigram = key.asInstanceOf[BiGramKey];
     math.abs(bigram.w1.hashCode() % numPartitions)
   }
}

object BiGramKey {
  implicit def orderingByFirstWord[A <: BiGramKey] = new Ordering[A] {
    override def compare(x: A, y: A): Int = {
      if (x.w1.equals(y.w1)) {
        if (x.w2.equals("*")) {
          -1
        } 
        else {
          x.w2.compareTo(y.w2)
        }
      } else {
    	  x.w1.compareTo(y.w1)
      }
    }
  }
}

case class BiGramKey(w1: String, w2: String) {
  override def hashCode(): Int = {
    this.w1.hashCode() + this.w2.hashCode()
  }
    
  override def equals(other: Any) = other match {
    case that: BiGramKey => this.w1 == that.w1 && this.w2 == that.w2
    case _ => false
  }
}

object App {

  def main(args: Array[String]) {
    val config = new SparkConf;
    config.setMaster("local[4]");
    config.setAppName("Order Inversion");
    config.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(config);
    val numPartitions = 1;
    val K = 10;
    val lines = sc.textFile("/home/hany/data/input.txt", numPartitions)
	  val data = lines.map(r => { 
      r.split("\\s+").sliding(2).map(pair => 
        { 
            Seq(
                (new BiGramKey(pair(0), "*"),1), 
                (new BiGramKey(pair(0), pair(1)),1)
            )
        })
    })
    .flatMap(_.toIterator)
    .flatMap(_.toIterator)
    .mapPartitions(f => {
       val m = new ExternalAppendOnlyMap[BiGramKey, Int, Int]((v: Int) => v, _ + _, _ + _);
       f.foreach(r => m.insert(r._1, r._2));
       m.iterator
    },true)
    .repartitionAndSortWithinPartitions(new BigramPartitioner(numPartitions))
    .mapPartitions(f => {
      var result = new ArrayBuffer[(BiGramKey,Double)];
      var lastKey : Option[BiGramKey] = None
      var runningSum : Int = 0;
      var marginalCount : Int = 0;
      var bigramCount : Int = 0;
      var key : BiGramKey = null;
      var value : Int = 0;
      while (f.hasNext) {
        val (k,v) = f.next();
        key = k; value = v;
        if (lastKey.isDefined && key != lastKey) {
          if (lastKey.get.w2 == "*")
            marginalCount = runningSum;
          else {
        	  bigramCount = runningSum;
        	  result.+=((lastKey.get, bigramCount.doubleValue()/marginalCount))            
          }
          runningSum = 0;
        } 
        runningSum += value;
        lastKey = Some(key);
      }
      result.+=((key, 1.0 * runningSum/marginalCount));        
      result.iterator
    }, true)
    .foreach(println)
  }
}
