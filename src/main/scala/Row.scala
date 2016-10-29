package koalas.row

import koalas.datavalue._

case class Row(val paramMap: Map[String, DataValue]){
  def apply[T](column: String): T = paramMap(column).asInstanceOf[T]

  def select(column: String): DataValue = paramMap(column)
  def select(columns: String*): Row = Row(paramMap filterKeys columns.toSet)
  def select(columns: Set[String]): Row = Row(paramMap filterKeys columns)
  def select(columns: Iterable[String]): Row = Row(paramMap filterKeys columns.toSet)

  def +(kvPair: Tuple2[String, DataValue]): Row = Row(paramMap + kvPair)

  def ++(otherRow: Row): Row = Row(paramMap ++ otherRow.paramMap)
}
