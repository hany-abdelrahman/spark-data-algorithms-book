package com.spark.Experiments

import scala.collection.immutable.HashSet
import scala.collection.immutable.Set
import scala.collection.mutable.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.util.collection.ExternalAppendOnlyMap

class FriendsSuggestions extends Serializable {
    var originalFriends : Set[Int] = new HashSet[Int]
    // <a common friend, suggestions by common friend>
    var possibleFriends : HashMap[Int,Set[Int]] = new HashMap[Int,Set[Int]]
    
    def processPossibleFriends() : HashMap[Int,Set[Int]] = {
      // res is map of [suggestedFriends, set of common friends]
      val res = new HashMap[Int,Set[Int]];
      possibleFriends.foreach({case(k,v) => {
          v.foreach(i => {
             res.put(i, res.getOrElse(i, new HashSet[Int]).+(k)) 
          });
        }
      })
      res
    }
    
    def getSuggestedFriends() : HashMap[Int,Set[Int]] = {
      processPossibleFriends().filter({case (k,v) => !originalFriends.contains(k)})
    }
    override def toString(): String = "(" + originalFriends + ", " + possibleFriends + ")";

}

object App {

  def main(args: Array[String]) {

    def combineTwoFriendsSuggestions(a : FriendsSuggestions, b : FriendsSuggestions) : FriendsSuggestions = {
      val c = new FriendsSuggestions;
      c.originalFriends = a.originalFriends.++(b.originalFriends)
      c.possibleFriends = a.possibleFriends.++(b.possibleFriends)
      c
    }
    
    val config = new SparkConf;
    config.setMaster("local[4]");
    config.setAppName("test");
    config.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(config);
    val numPartitions = 1;
    val K = 5;
    val lines = sc.textFile("/home/hany/data/friends.txt", numPartitions)
    val data = lines.map(l => {
      val parts = l.split("\\s+"); 
      (parts(0).toInt, parts(1))
    })
    .map({case(uid,friends) => {
        val map = new ExternalAppendOnlyMap[Int, FriendsSuggestions, FriendsSuggestions](
            (v:FriendsSuggestions) => v,
            (a,b) => combineTwoFriendsSuggestions(a, b),
            (a,b) => combineTwoFriendsSuggestions(a, b)
        );
        val parts = friends.split(",").map(i=>i.toInt).toSet
        val s = new FriendsSuggestions;
        s.originalFriends = parts
        map.insert(uid, s);
        parts.foreach(i => {
          val s = new FriendsSuggestions;
          val m = new HashMap[Int,Set[Int]];
          m.+=((uid, parts.-(i)))
          s.possibleFriends = m
          map.insert(i, s);
        });
        map.iterator
      }
    })
    .flatMap(r => r)
    .combineByKey[FriendsSuggestions](
        (v:FriendsSuggestions) => v,
        (a,b) => combineTwoFriendsSuggestions(a, b),
        (a,b) => combineTwoFriendsSuggestions(a, b)
     )
     .mapValues(_.getSuggestedFriends())
     .foreach(println)
  }
}
