package koalas.dataframe

import koalas.row.Row
import koalas.datavalue._
import koalas.schema._
import koalas.csvfile.CSVFile
import koalas.series.Series

final class DataFrame(override val values: Vector[Row]) extends Series[Row](values) {
  def select[T](column: String): Series[T] = Series(values.map(row => row[T](column)))

  def mapDF(f: Row => Row): DataFrame = DataFrame(values.map(f))
  override def filter(p: Row => Boolean): DataFrame = DataFrame(values.filter(p))
  override def groupBy[R](f: Row => R): Map[R, DataFrame] = values.groupBy(f).mapValues(DataFrame(_))
  override def partition(p: Row => Boolean): (DataFrame, DataFrame) = {
    val (left, right) = values.partition(p)
    (DataFrame(left), DataFrame(right))
  }
  // def update(column: string: values: Series[DataValue]): DataFrame
  // def update(column: string: value: DataValue): DataFrame
  def sum(column: String): NumericalValue =
    values.reduce(DataFrame.sumRowElement(column, _: Any, _: Any))
}

final object DataFrame{
  def apply(rows: Vector[Row]): DataFrame = new DataFrame(rows)
  def apply(rows: Iterable[Row]): DataFrame = new DataFrame(rows.toVector)
  def apply(columnMap: Map[String, Vector[DataValue]]): DataFrame = {
    val length: Int = columnMap(columnMap.keysIterator.next).length
    def buildRow(i: Int): Row = Row(columnMap.map(column => column._1 -> column._2(i)))
    val rowVector: Vector[Row] = Range(0, length).map(buildRow).toVector

    new DataFrame(rowVector)
  }
  // def apply(columnMap: Map[String, Iterable[DataValue]]): DataFrame
  // def apply(columnMap: Map[String, Series[DataValue]]) = {
  //   new DataFrame(rowIterable)
  // }
  private def normalizeToNumerical(a: Any): NumericalValue = {
    a match {
      case _: Row => a.asInstanceOf[Row][NumericalValue](column)
      case _: NumericalValue => a
      case _ => throw new RuntimeException("method sum may only be applied to a column of " +
        "values of type NumericalValue")
    }
  }

  private def sumRowElement(column: String, a: Any, b: Any) = {
    val numA: NumericalValue = normalizeToNumerical(a)
    val numB: NumericalValue = normalizeToNumerical(b)
    numA + numB
  }

  /**
   * This method instantiates a new DataFrame from a csv file
   * 
   * @param filePath
   * @param schema
   * @param delimiter
   * @param header
   * @param readerType
   */
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
