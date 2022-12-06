package lyricgen

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import spire.math.Polynomial.x

import scala.io._
import scala.util.Random
import _root_.scala.collection.mutable



object LyricBagsJen {

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("A6").setMaster("local[4]")
    val sc = new SparkContext(conf)

    System.setProperty("hadoop.home.dir", "c:/winutils/")
    // TODO: get all bags from all lyrics files. Example:
    val exampleLyric = "hello these are awesome\nlyrics, and you will\nend up loving, this,\nsong\n\nand you can't sing them\n\nboom boom boom\nscooby doo pa pa"
    val exampleBag = bagOf(readLyrics(exampleLyric, 2), 2)

    //    println(exampleBag.mkString("\n"))

    val songsContent = readFilesContent("src/main/scala/brunomarslyrics.txt")

    //have a global bag map of all the bags generates from each song
    val globalBag = Map[List[String], List[String]]();
    var rddGlobalBag = sc.parallelize(globalBag.toList)
    songsContent.foreach({case(x, y) =>
      val curBag = bagOf(readLyrics(y, 2), 2)
      val rddCurBag = sc.parallelize((curBag.toList))
      rddGlobalBag = rddCurBag.union(rddGlobalBag)
    })

    rddGlobalBag.groupByKey().map({case (k, v) => (k, v.toList.flatMap(x => x).distinct)}).foreach(println(_))

    // TODO: Generate the lyrics from this final bag
  }

  def generateLyrics(bag: Map[List[String], List[String]]) = {
    // Choose one word from <START>, currWord

    // Loop until you reach a word with <END> in value list
    // Get value list of currWord
    // Make a random choice from value list, choice
    // Add currWord to string
    // Check if word is <N> then add a \n
    // else add the word as is
    // set currWord equal to choice

    // return string
  }

  // Function reads in a text file with song lyrics, and cleans the lyrics from empty lines,
  // and ghost lyrics [those parts between ()] for when we are making the bag of words.
  // I/O: String => Array[String]
  def readLyrics(song_lyrics: String, N: Int): List[String] = {
    // read in a string of lyrics
    //split on '\n'
    val l = song_lyrics.split('\n')
      .filter { // Remove all empty lines and those lines that say [Verse 1], [Chorus], etc...
        l => !(l.contains("[") | l.isEmpty)
      }
      .map(_.toLowerCase()) // TODO: decide if we get better lyrics with case preserved or not
      .map(_ + " <N>") // Adding <N> to every line
      .flatMap(l => l.split(" "))
      .map(word => word
        .replace("(", "")
        .replace(")", "")
        .replace(",", ""))
      .toList

    // Adding START
    var nones = (0 to N - 1).toList
    var lyrics = "<START>" :: l

    // Adding END to the last line
    var lyrics_ = lyrics.toArray
    val lengthLyrics = lyrics.length - 1
    lyrics_(lengthLyrics) = "<END>"

    lyrics_.toList
  }

  // Function creates a map representing a bag of N words
  // This includes the <START>, <N>, <END> states
  // I/O: List[String] => Map[List[String], List[String]]
  def bagOf(lyrics: List[String], N: Int): Map[List[String], List[String]] = {
    // Making the bags
    val orders = (0 until lyrics.length - N).toList
      .map(l => ((l until l + N).toList -> (l + N)))
      .map {
        case (k, v) => k.map(x => lyrics(x)) -> lyrics(v)
      }
      .groupBy { case (k, v) => k }
      .mapValues(l => l.map(t => t._2).distinct)

    orders
  }

  // Function returns a random element in a List
  // I/O: List[Any] => Any
  def randomChoice(list: List[Any]): Any = {
    val random = new Random()
    list(random.nextInt(list.length))
  }

  //Function concatenates the whole lyrics into one string and puts it in a map
  def readFilesContent(fileName: String): mutable.Map[Int, String] = {
    val songsMap = mutable.Map[Int, String]();
    val l = Source.fromFile(fileName)
      .getLines
      .toArray
    var lyricString = ""
    var count = 1
    for (i <- 0 to l.size - 1) {
      if(l(i).trim() != "") //if its not an empty line then we are not at the end of the song
        lyricString = lyricString + l(i).trim() + '\n'
      else {
        songsMap += (count -> lyricString)
        lyricString = ""
        count = count + 1
      }
    }
    songsMap
  }

}
