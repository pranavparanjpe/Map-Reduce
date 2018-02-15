class ArtistTerms(row : String) {

  val columns = row.split(";") //All the columns from the row passed in the constructor

  val artistId : String = columns(0)
  val artistTerm : String = columns(1)

  def getRecords() = (artistId , artistTerm)

}