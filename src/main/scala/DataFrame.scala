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
  //
  // def fromCSV(filePath: String, schema: String) {
  //   val data: List[List[DataValue]] = CSVFile.read(filePath, schema)
  //   val rowVector: Vector[Row] =
  //
  //   new DataFrame(rowVector)
  // }
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
