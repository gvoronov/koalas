package dataframe

import row.Row
import datavalue._
import schema._
import csvfile.CSVFile
import series.Series

class DataFrame(override val values: Vector[Row]) extends Series[Row](values) {
  def select[T](column: String): Series[T] = Series(values.map(row => row[T](column)))

  def mapDF(f: Row => Row): DataFrame = DataFrame(values.map(f))
  override def filter(p: Row => Boolean): DataFrame = DataFrame(values.filter(p))
  override def groupBy[R](f: Row => R): Map[R, DataFrame] = values.groupBy(f).mapValues(DataFrame(_))
  override def partition(p: Row => Boolean): (DataFrame, DataFrame) = {
    val (left, right) = values.partition(p)
    (DataFrame(left), DataFrame(right))
  }
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

  def fromCSV(
      filePath: String, schema: Schema,  delimiter: String = ",",
      header: Boolean = true, readerType: String = "File"): DataFrame = {
    // def imapToList(imap: Map[String, String]): List[String] =
    //   List.range(0, schema.length).map(icol => imap(icol.toString))

    lazy val imapToList: Map[String, String] => List[String] =
      (imap: Map[String, String]) =>
      List.range(0, schema.length).map(icol => imap(icol.toString))
    val data: List[Map[String, String]] = CSVFile.read(filePath, delimiter, header, readerType)

    val rowVector: Vector[Row] = if (header)
      data.map(schema(_)).toVector
    else
      data.map(imapToList).map(schema(_)).toVector

    new DataFrame(rowVector)
  }
}
