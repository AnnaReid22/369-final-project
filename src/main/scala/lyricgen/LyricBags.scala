package lyricgen

import scala.List
import scala.io._

object LyricBags {

  def main (args : Array[String]) = {
    println(readLyrics("src/main/scala/lyricgen/antihero.txt").mkString("\n"))
  }

  // Function reads in a text file with song lyrics, and cleans the lyrics from empty lines,
  // and ghost lyrics [those parts between ()] for when we are making the bag of words.
  // I/O: String => Array[String]
  def readLyrics(path : String) : Array[String] = {
    val lyrics = Source.fromFile(path)
      .getLines
      .toArray
      .filter { // Remove all empty lines and those lines that say [Verse 1], [Chorus], etc...
        l => !(l.contains("[") | l.isEmpty)}
      .map(_.toLowerCase()) // TODO: decide if we get better lyrics with case preserved or not
      .map(_ + " <N>") // Adding <N> to every line

    // Adding START to the first line
    lyrics(0) = "<START> " + lyrics(0)

    // Adding END to the last line
    val lastIndex = lyrics.length - 1
    val lastLength = lyrics(lastIndex).length // Used to remove the <N> to the last line
    lyrics(lastIndex) = lyrics.last.substring(0, lastLength - 4) + " <END>"

    lyrics
  }

  // Function creates a map representing a bag of words of N = 2
  // This includes the <START>, <N>, <END> states
  // I/O: Array[String] => (String, List(String))
  def bagOf2(lyrics : List[String])  = {
    // Making the bags

  }

}
