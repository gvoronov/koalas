package koalas.dataframe

import koalas.row.Row
import koalas.datavalue._
import koalas.schema._
import koalas.csvfile.CSVFile
import koalas.series.Series

final class DataFrame(override val values: Vector[Row]) extends Series[Row](values) {
  def apply[T](column: String): Series[T] = Series(values.map(row => row[T](column)))
  override def apply(subset: Series[Boolean]): DataFrame =
    DataFrame(values.zip(subset.values).filter(_._2).map(_._1))

  def select[T](column: String): Series[T] = Series(values.map(row => row[T](column)))
  def irow(i: Int): Row = values(i)

  def map(f: Row => Row): DataFrame = DataFrame(values.map(f))
  // def mapDF(f: Row => Row): DataFrame = DataFrame(values.map(f))
  override def filter(p: Row => Boolean): DataFrame = DataFrame(values.filter(p))
  override def groupBy[R](f: Row => R): Map[R, DataFrame] = values.groupBy(f).mapValues(DataFrame(_))
  override def partition(p: Row => Boolean): (DataFrame, DataFrame) = {
    val (left, right) = values.partition(p)
    (DataFrame(left), DataFrame(right))
  }

  def +(that: Row): DataFrame = DataFrame(values :+ that)
  def ++(that: DataFrame): DataFrame = DataFrame(values ++ that.values)

  def update(column: String, value: DataValue): DataFrame = map(_.update(column, value))
  def update(column: String, colValues: Series[DataValue]): DataFrame = DataFrame(
    values.zip(colValues.values).map(zval => zval._1.update(column, zval._2))
  )

  def sum(column: String): NumericalValue =
    values.asInstanceOf[Vector[Any]].reduce(DataFrame.sumRowElement(column)_)
    .asInstanceOf[NumericalValue]
}

/** Factory for [[koalas.dataframe.DataFrame]] instances. */
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
  private def normalizeToNumerical(column: String, a: Any): NumericalValue = {
    a match {
      case _: Row => a.asInstanceOf[Row][NumericalValue](column)
      case _: NumericalValue => a.asInstanceOf[NumericalValue]
      case _ => throw new RuntimeException("method sum may only be applied to a column of " +
        "values of type NumericalValue")
    }
  }

  private def sumRowElement(column: String)(a: Any, b: Any): Any = {
    val numA: NumericalValue = normalizeToNumerical(column, a)
    val numB: NumericalValue = normalizeToNumerical(column, b)
    (numA + numB).asInstanceOf[Any]
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

  /* Create an empty DataFrame */
  def empty() = new DataFrame(Vector.empty: Vector[Row])
}
