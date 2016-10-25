class DataFrame(val rowVector: Vector[Row]) extends Series[Row](rowVector) {

}

object DataFrame{
  def apply(rowVector: Vector[Row]): DataFrame = new DataFrame(rowVector)
  def apply(columnMap: Map[String, Vector[DataValue]]) = {
    val length: Int = columnMap(columnMap.keysIterator.next).length
    def buildRow(i: Int): Row = Row(columnMap.map(column => column._1 -> column._2(i)))
    val rowVector: Vector[Row] = Range(0, length).map(buildRow).toVector

    new DataFrame(rowVector)
  }
  // def apply(columnMap: Map[String, Series[DataValue]]) = {
  //   new DataFrame(rowIterable)
  // }

  // Eventaully pass other CSVFile.read params
  // Need to figure out how to pass a schema that only picks a subset of cols
  def fromCSV(filePath: String, schema: Schema,  delimiter: String = ",", header: Boolean = true) {
    // def imapToList(imap: Map[String, String]): List[String] =
    //   List.range(0, schema.length).map(icol => imap(icol.toString))

    lazy val imapToList: Map[String, String] => List[String] =
      (imap: Map[String, String]) =>
      List.range(0, schema.length).map(icol => imap(icol.toString))
    val data: List[Map[String, String]] = CSVFile.read(filePath, delimiter, header)

    val rowVector: Vector[Row] = if (header)
      data.map(schema(_)).toVector
    else
      data.map(imapToList).map(schema(_)).toVector

    new DataFrame(rowVector)
  }

  // private def imapToList(imap: Map[String, String]): List[String] =
  //   List.range(0, schema.length).map(icol => imap(icol.toString))
}



// import scala.collection.immutable.LinearSeq

// class DataFrame extends LinearSeq[Row]
// class DataFrame(val rows: LinearSeq[Row]) extends LinearSeq[Row] {
//   def apply(idx: Int): Row = rows(idx)
//   def length: Int = rows.length
// }
//
// // object DataFrame {
// //   def apply() = new DataFrame
// // }
