package lyricgen

import scala.io._
import scala.util.Random

object LyricBags {

  def main (args : Array[String]) = {

    // TODO: get all bags from all lyrics files. Example:
    val exampleBag = bagOf(readLyrics("src/main/scala/lyricgen/example.txt", 2), 2)

    println(exampleBag.mkString("\n"))

    // TODO: aggregate all maps of bags together to get the final bag

    // TODO: Generate the lyrics from this final bag
  }

  def generateLyrics(bag : Map[List[String], List[String]]) = {
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
  def readLyrics(path : String, N : Int) : List[String] = {
    val l = Source.fromFile(path)
      .getLines
      .toArray
      .filter { // Remove all empty lines and those lines that say [Verse 1], [Chorus], etc...
        l => !(l.contains("[") | l.isEmpty)}
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
  def bagOf(lyrics : List[String], N : Int) : Map[List[String], List[String]]  = {
    // Making the bags
    val orders = (0 until lyrics.length - N).toList
      .map(l => ((l until l + N).toList -> (l + N)))
      .map{
        case (k, v) => k.map(x => lyrics(x)) -> lyrics(v)
      }
      .groupBy{ case (k, v) => k}
      .mapValues(l => l.map(t => t._2))

    orders
  }

  // Function returns a random element in a List
  // I/O: List[Any] => Any
  def randomChoice(list: List[Any]): Any = {
    val random = new Random()
    list(random.nextInt(list.length))
  }

}
