import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection._
import scala.io._


// calculate per region the total streams for each song
// weight each region relative to all other regions
// for each song in each region, calculate the relative stream weight for each region
// sum all of the weights for each song
// the resulting number is the relative influence for each song

object kneighbors {

  def main(args: Array[String]): Unit = {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]")
  val sc = new SparkContext(conf)

  val file = sc.textFile("src/main/final.csv")
    .map(x => x.split("(,(?=\\S))"))
    .filter(x => x.contains("top200"))
    .filter(x => x.length == 9)
    .persist()

  val region_streams = file
    .map(x => (x(5), x(8).toDouble))
    .reduceByKey((x, y) => x + y)

  val total_streams = region_streams.values.sum

  val region_streams_map = region_streams.collectAsMap()

  val region_weights = region_streams
    .mapValues(x => x / total_streams).collectAsMap()

  val song_region_weights = file
    // (songTitle, (region, streams))
    .map(x => (x(0), (x(5), x(8).toDouble)))
    // (songTitle, (region, weighted_streams))
    // weighted_streams = (song_streams / total_region_streams) * total_region_weight
    .mapValues(x => (x._2 / region_streams_map(x._1)) * region_weights(x._1))
    .reduceByKey((x, y) => x + y)
    .sortBy(x => -1 * x._2)
    .take(10)
    .foreach(x => println(x))
  }
}

//Top10!!
//(Shape of You,0.0046756249240189895)
//(Blinding Lights,0.004620128103403348)
//(Dance Monkey,0.004058592661078816)
//(Someone You Loved,0.003565521325320671)
//(Sunflower - Spider-Man: Into the Spider-Verse,0.0033747196168977924)
//(Señorita,0.0031963043498186108)
//(bad guy,0.0030916898863656726)
//(Don't Start Now,0.002902405108366854)
//(Lucid Dreams,0.0028387204358656685)
//(Happier,0.002831759326071754)