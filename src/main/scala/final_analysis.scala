import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection._
import scala.io._


object final_analysis {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // line,title,artist,top genre,year,bpm,enrgy,dance,dB,live,val,dur,acous,spch,pop
    // (songName, artist, genre, year, bpm, energy, dance, dB)
    val top10 = sc.textFile("src/main/top10.csv")
      .map(_.split(","))
      .map(x => (x(1), x(2), x(3), x(4), x(5).toInt, x(6).toInt, x(7).toInt, x(8).toInt))
      .persist()

    // (year, bpm)
    println("BPM:")
    top10.map(x => (x._4, x._5))
      .groupByKey()
      .mapValues(x => calculateAverage(x.toList))
      .sortBy(x => x._1).collect()
      .foreach(x => println(x._1 + f", ${x._2}%1.2f"))

    // (year, energy)
    println("Energy:")
    top10.map(x => (x._4, x._6))
      .groupByKey()
      .mapValues(x => calculateAverage(x.toList))
      .sortBy(x => x._1).collect()
      .foreach(x => println(x._1 + f", ${x._2}%1.2f"))

    // (year, dance)
    println("Dance:")
    top10.map(x => (x._4, x._7))
      .groupByKey()
      .mapValues(x => calculateAverage(x.toList))
      .sortBy(x => x._1).collect()
      .foreach(x => println(x._1 + f", ${x._2}%1.2f"))

    // (year, dB)
    println("dB (Loudness):")
    top10.map(x => (x._4, x._8))
      .groupByKey()
      .mapValues(x => calculateAverage(x.toList))
      .sortBy(x => x._1).collect()
      .foreach(x => println(x._1 + f", ${x._2}%1.2f"))


    // title, rank, date, artist, url, region, chart, trend, streams
    // ((songName, songArtist), streams)
    val charts = sc.textFile("src/main/final.csv")
      // https://stackoverflow.com/questions/18893390/splitting-on-comma-outside-quotes
      .map(x => (x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1), x.substring((x.lastIndexOf(",") + 1))))
      .filter(x => x._2 != "")
      .map(x => ((x._1(0), x._1(3)), x._2))
      .mapValues(x => x.toInt)
      .reduceByKey({(x, y) => x + y})
      .persist()

    //combine charts
    val charts_key = charts.map(x => (x._1, x._2))
    val top10_key = top10.map(x => ((x._1, x._2), (x._3, x._4, x._5, x._6, x._7, x._8)))
    val merged = charts_key.join(top10_key)
      .sortBy(x =>(x._1._2, -1 * x._2._1, x._1._1))
//    merged.collect().distinct.foreach(x => println(x))

    merged.map(x => (x._1._2, x._2._1))
      .countByKey().toSeq.sortBy(x => -1 * x._2).take(10)
      .foreach(x => println(x))

  }

  def calculateAverage(data: List[Int]): Double = {
    val res = data.aggregate((0, 0))((x, y) => (
      x._1 + y, x._2 + 1),
      (x, y) => (x._1 + y._1, x._2 + y._2))
    res._1.toDouble / res._2.toDouble
  }
}