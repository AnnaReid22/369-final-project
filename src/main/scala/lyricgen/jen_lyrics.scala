package lyricgen

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ListBuffer
import scala.io._
import scala.util.Random
import _root_.scala.collection.mutable
import scala.xml.NodeSeq.Empty.text



object LyricBagsJen {

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("A6").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val N = 2;

//    System.setProperty("hadoop.home.dir", "c:/winutils/")

    val songsContent = readFilesContent("src/main/scala/lyricgen/brunomarslyrics.txt")

    //have a global bag map of all the bags generates from each song
    val globalBag = Map[List[String], List[String]]();
    var rddGlobalBag = sc.parallelize(globalBag.toList)
    songsContent.foreach({case(x, y) =>
      val curBag = bagOf(readLyrics(y, N), N)
      val rddCurBag = sc.parallelize((curBag.toList))
      rddGlobalBag = rddCurBag.union(rddGlobalBag)
    })

    val bigBag = rddGlobalBag.groupByKey().map({case (k, v) => (k.mkString(", "), v.toList.flatMap(x => x).distinct)}).collect().toMap

    // TODO: Generate the lyrics from this final bag
    val file = new File("generatedLyrics.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(generateLyrics(bigBag))
    bw.close()

  }

  // Function generates lyrics given a bag of words.
  // I/O: Map[String, List[String]] => String
  def generateLyrics(bag: Map[String, List[String]]): String = {
    var generatedLyrics = ListBuffer[String]()
    // Choose one word from null, <START>, set to currWord
    var prevWord = "<START>"
    var currWord = randomChoice(bag("null, <START>"))

    // Construct key
    var currKey = prevWord + ", " + currWord

    // Loop until you reach a word with <END> in value list
    while(! bag(currKey).contains("<END>")) {
      // Get value list of currWord
      // Make a random choice from value list, choice
      prevWord = currWord
      currWord = randomChoice(bag(currKey))

      // Add word to generatedLyrics
      if (prevWord == "<N>") {
        generatedLyrics += "\n"
      } else {
        generatedLyrics += prevWord
      }
      currKey = prevWord + ", " + currWord
    }
    generatedLyrics.mkString(" ")
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
    val nones = (0 until N - 1).map(_ => "null").toList
    val lyrics = "null" :: "<START>" :: l

    // Adding END to the last line
    val lyrics_ = lyrics.toArray
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
  def randomChoice(list: List[String]) : String = {
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
