
import org.apache.spark.{SparkContext,SparkConf}
import java.io._
import scala.compat.Platform.EOL

object Main {

  val sc = new SparkContext(new SparkConf().setAppName("Spark Tests").setMaster("local"))

  def main(args: Array[String]): Unit = {

    val songInfo=sc.textFile(args(0))
    val artistFile=sc.textFile(args(1))
    val qset=args(2)
    //val qset="small"
    var path="output/small";
    var timeFile = new File("output/small/runtime.txt");
    if (qset=="large"){
       path="output/large"
       timeFile = new File("output/large/runtime.txt")
    }

    //val songInfo=sc.textFile("input/song_info.csv")
    //val artistFile=sc.textFile("input/artist_terms.csv")
    val timediff = new BufferedWriter(new FileWriter(timeFile))
    val queries=new Query(sc)

    val songsInfoData = songInfo.mapPartitionsWithIndex {
    //val songsInfoData = sc.textFile(songInfo).mapPartitionsWithIndex {
      case (0, iter) => iter.drop(1)
      case (_, iter) => iter
    }.map(row => new Songs(row)).persist()

    val artistTermsInfo = artistFile.mapPartitionsWithIndex {
    //val artistTermsInfo = sc.textFile(artistFile).mapPartitionsWithIndex {
      case (0, iter) => iter.drop(1)
      case (_, iter) => iter
    }.map( row => new ArtistTerms(row).getRecords)

    //Query 1

    var opFile=path+"/query1.txt"
    var br = new BufferedWriter(new FileWriter(opFile))

    var t0 = System.nanoTime()
    val DistinctArtist = queries.artistCount(songsInfoData)
    val DistinctSong=queries.songCount(songsInfoData)
    val DistinctAlbum=queries.albumCount(songsInfoData)

    var t1 = System.nanoTime()
    timediff.write("\nQuery:Time(in milliseconds)\n")
    timediff.write(EOL+"\nQuery1: " + (t1 - t0)/1000000 + "\n")
    timediff.flush()

    val outString = StringBuilder.newBuilder
    br.write(EOL+"\nDistinct Artists Count: " + DistinctArtist)
    br.write(EOL+"\nDistinct Songs Count: " + DistinctSong)
    br.write(EOL+"\nDistinct Album Count: " + DistinctAlbum)
    br.close()

    //Query2 Loudest
    opFile=path+"/query2.txt"
    br = new BufferedWriter(new FileWriter(opFile))
    t0 = System.nanoTime()

    val topLoudSongs=queries.getLoudSongs(songsInfoData)
    t1 = System.nanoTime()
    timediff.write(EOL+"Query2: " + (t1 - t0)/1000000 + "\n")
    timediff.flush()
    br.write(EOL+"\nTitle,Song Id,Song Loudness\n"+EOL)
    topLoudSongs.foreach(s => br.write(s._1._1.toString().replace(",","").replaceAll("[^A-Za-z0-9 ]","") +","+
      s._1._2.toString().replace(",","").replaceAll("[^A-Za-z0-9 ]","")+","
      +s._2.toString.replace(",","") + EOL))
    br.close()

    //Query3 Longest
    opFile=path+"/query3.txt"
    br = new BufferedWriter(new FileWriter(opFile))
    t0 = System.nanoTime()

    val topLongestSongs=queries.getLongestSongs(songsInfoData)
    t1 = System.nanoTime()
    timediff.write(EOL+"Query3: " + (t1 - t0)/1000000 + "\n")
    timediff.flush()
    br.write(EOL+"\nTitle,Song Id,Song Duration\n"+EOL)
    topLongestSongs.foreach(s => br.write(s._1._1.toString().replace(",","").replaceAll("[^A-Za-z0-9 ]","")+","+
      s._1._2.toString().replace(",","").replaceAll("[^A-Za-z0-9 ]","")+","
      +s._2.toString.replace(",","") + EOL))
    br.close()

    //Query4 Fastest
    t0 = System.nanoTime()
    opFile=path+"/query4.txt"
    br = new BufferedWriter(new FileWriter(opFile))

    val topFastSongs=queries.getFastSongs(songsInfoData)
    t1 = System.nanoTime()
    timediff.write(EOL+"Query4: " + (t1 - t0)/1000000 + "\n")
    timediff.flush()
    br.write(EOL+"\nTitle,Song Id,Tempo\n"+EOL)
    topFastSongs.foreach(s => br.write(s._1._1.toString().replace(",","").replaceAll("[^A-Za-z0-9 ]","")+","+
      s._1._2.toString().replace(",","").replaceAll("[^A-Za-z0-9 ]","")+","
      +s._2.toString.replace(",","") + EOL))
    br.close()

    //Query5 Familiar artist
    opFile=path+"/query5.txt"
    br = new BufferedWriter(new FileWriter(opFile))
    t0 = System.nanoTime()

    val topFamiliarArtist=queries.getFamiliarArtist(songsInfoData)
    t1 = System.nanoTime()
    timediff.write(EOL+"Query5: " + (t1 - t0)/1000000 + "\n")
    timediff.flush()
    br.write(EOL+"\nArtistId,Name,Familiarity\n"+EOL)
    topFamiliarArtist.foreach(s => br.write(s._1._1.toString().replace(",","").replaceAll("[^A-Za-z0-9 ]","")+","+
      s._1._2.toString().replace(",","").replaceAll("[^A-Za-z0-9 ]","")+","
      +s._2.toString.replace(",","") + EOL))
    br.close()

    //Query6 Hottest Song
    opFile=path+"/query6.txt"
    br = new BufferedWriter(new FileWriter(opFile))
    t0 = System.nanoTime()

    val topHottestSongs=queries.getHottestSongs(songsInfoData)
    t1 = System.nanoTime()
    timediff.write(EOL+"Query6: " + (t1 - t0)/1000000 + "\n")
    timediff.flush()
    br.write(EOL+"\nTitle,Song Id,Hottness\n"+EOL)
    topHottestSongs.foreach(s => br.write(s._1._1.toString().replace(",","").replaceAll("[^A-Za-z0-9 ]","")+","+
      s._1._2.toString().replace(",","").replaceAll("[^A-Za-z0-9 ]","")+","
      +s._2.toString.replace(",","") + EOL))
    br.close()

    //Query7 Hottest Artist
    opFile=path+"/query7.txt"
    br = new BufferedWriter(new FileWriter(opFile))
    t0 = System.nanoTime()

    val topHottestArtist=queries.getHottestArtists(songsInfoData)
    t1 = System.nanoTime()
    timediff.write(EOL+"Query7: " + (t1 - t0)/1000000 + "\n")
    timediff.flush()
    br.write(EOL+"\nTitle,Artist Id,Hottness\n"+EOL)
    topHottestArtist.foreach(s => br.write(s._1._1.toString().replace(",","").replaceAll("[^A-Za-z0-9 ]","")+","+
      s._1._2.toString().replace(",","").replaceAll("[^A-Za-z0-9 ]","")+","
      +s._2.toString.replace(",","") + EOL))
    br.close()


    //Query8 Hottest Genres
    opFile=path+"/query8.txt"
    br = new BufferedWriter(new FileWriter(opFile))
    t0 = System.nanoTime()

    val topHottestGenres=queries.getHottestGenres(songsInfoData,artistTermsInfo)
    t1 = System.nanoTime()
    timediff.write(EOL+"Query8: " + (t1 - t0)/1000000 + "\n")
    timediff.flush()
    br.write(EOL+"\nArtist Term,Average Hottness\n"+EOL)
    topHottestGenres.foreach(s => br.write(s._1.toString().replace(",","").replaceAll("[^A-Za-z0-9 ]","")+","
      +s._2.toString.replace(",","") + EOL))
    br.close()

    //Query9 Popular keys with confidence >0.7
    opFile=path+"/query9.txt"
    br = new BufferedWriter(new FileWriter(opFile))
    t0 = System.nanoTime()

    val topKeys=queries.gettopKeys(songsInfoData)
    t1 = System.nanoTime()
    timediff.write(EOL+"Query9: " + (t1 - t0)/1000000 + "\n")
    timediff.flush()
    br.write(EOL+"\nKey,Count\n"+EOL)
    topKeys.foreach(s => br.write(s._1.toString().replace(",","").replaceAll("[^A-Za-z0-9 ]","")+","
      +s._2.toString.replace(",","") + EOL))
    br.close()


    //Query10 5 most prolific artists
    opFile=path+"/query10.txt"
    br = new BufferedWriter(new FileWriter(opFile))
    t0 = System.nanoTime()

    val topProfilicArtists=queries.gettopProfilicArtists(songsInfoData)
    t1 = System.nanoTime()
    timediff.write(EOL+"Query10: " + (t1 - t0)/1000000 + "\n")
    timediff.flush()
    br.write(EOL+"\nArtist Id,Name,Count\n"+EOL)
    topProfilicArtists.foreach(s => br.write(s._1._1.toString().replace(",","").replaceAll("[^A-Za-z0-9 ]","")+","+
      s._1._2.toString().replace(",","").replaceAll("[^A-Za-z0-9 ]","")+","
      +s._2.toString.replace(",","") + EOL))
    br.close()


    //Query11 Common words
    opFile=path+"/query11.txt"
    br = new BufferedWriter(new FileWriter(opFile))
    t0 = System.nanoTime()

    val MostCommonWords=queries.getMostCommonWords(songsInfoData)
    t1 = System.nanoTime()
    timediff.write(EOL+"Query11: " + (t1 - t0)/1000000 + "\n")
    timediff.flush()
    br.write(EOL+"\nWord,Count\n"+EOL)
    MostCommonWords.foreach(s => br.write(s._1.toString().replace(",","").replaceAll("[^A-Za-z0-9 ]","")+","
      +s._2.toString.replace(",","") + EOL))
    br.close()
  }

}
