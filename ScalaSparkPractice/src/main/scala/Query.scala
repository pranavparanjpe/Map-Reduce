import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

class Query(sc: SparkContext) {

  def artistCount(songData: RDD[Songs]): Long = {
    songData
      .map(_.artistId)
      .filter(v => !v.isEmpty)
      .distinct()
      .count()
  }

  def songCount(songData: RDD[Songs]): Long = {
    songData.map(_.songId).distinct.count()
  }

  def albumCount(songData: RDD[Songs]): Long = {
    songData
      .map(line => (line.artistId, line.album))
      .filter(tuple =>
        !tuple._1.isEmpty &&
          !tuple._2.isEmpty)
      .distinct()
      .count()

  }

  def getLoudSongs(songData: RDD[Songs]): Array[((String, String), Float)] = {
    songData
      .map(line => ((line.title, line.songId), line.loudness)).sortBy(_._2, ascending = false).take(5)
  }

  def getLongestSongs(songData: RDD[Songs]): Array[((String, String), Float)] = {
    songData
      .map(line => ((line.title, line.songId), line.duration)).sortBy(_._2, ascending = false).take(5)
  }


  //Get Fastest songs-tempo
  def getFastSongs(songData: RDD[Songs]): Array[((String, String), Float)] = {
    songData
      .map(line => ((line.title, line.songId), line.tempo)).sortBy(_._2, ascending = false).take(5)
  }

  //Get artist familiarity
  def getFamiliarArtist(songData: RDD[Songs]): Array[((String, String), Float)] = {
    songData
      .map(line => ((line.artistId, line.artist_name), line.artist_familiarity)).distinct().sortBy(_._2, ascending = false).take(5)
  }


  //Get Hottest Songs
  def getHottestSongs(songData: RDD[Songs]): Array[((String, String), Float)] = {
    songData
      .map(line => ((line.title, line.songId), line.song_hotttnesss)).distinct().sortBy(_._2, ascending = false).take(5)
  }

  //Get Hottest Artists
  def getHottestArtists(songData: RDD[Songs]): Array[((String, String), Float)] = {
    songData
      .map(line => ((line.artistId, line.artist_name), line.artist_hotttnesss)).distinct().sortBy(_._2, ascending = false).take(5)
  }

  //Get Artist Genres
  def getHottestGenres(songData: RDD[Songs], artistData: RDD[(String, String)]): Array[(String, Float)] = {
    val sdata = songData
      .map(line => (line.artistId, line.artist_hotttnesss))

    sdata.join(artistData)
      .map { case (artistId, (artistHotttnesss, artistTerm)) => (artistTerm, artistHotttnesss) }
      .mapValues((_, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(y => y._1 / y._2).sortBy(_._2,ascending = false).take(5)

  }

  //Get keys
  def gettopKeys(songData: RDD[Songs]): Array[(Int, Int)] = {
    songData
        .filter(line=>(line.key_confidence>0.7))
        .map(line=>(line.key,1))
        .reduceByKey((x,y)=>x+y)
      .sortBy(_._2, ascending = false).take(5)
  }

  //Get profilic artists did not change first query written in A6 by Rashmi
  def gettopProfilicArtists(songData: RDD[Songs]): Array[((String,String),Int)] = {
    songData
      .map(line => ((line.artistId, line.artist_name), line.songId))
        .groupByKey()
        .map{case(x:(String,String), y:Iterable[String]) => (x, y.iterator.length)}
      .sortBy(_._2,ascending = false).take(5)
  }

  //Get most common words did not change first query written in A6 by Rashmi
  def getMostCommonWords(songData: RDD[Songs]): Array[(String,Int)] = {
    val ignored = Set("that", "with", "the", "and", "to", "of", "a", "it", "she", "he", "you", "in", "i",
      "her", "at", "as", "on", "this", "for", "but", "not", "or","my","me")
    songData
        .flatMap(line=>line.title.filter(c => c.isLetter || c.isWhitespace)
          .toLowerCase.split(" ")
          .filter ( !_.isEmpty ))
      .filter(!ignored.contains(_))
      .map(word => (word,1)).reduceByKey((x,y) => x+y)
      .sortBy(_._2,ascending = false).take(5)
  }




}
