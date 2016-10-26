// class Row(params: List[Tuple3[String, Byte, Any]]){
//   val groupedParams = params.groupBy(_._2)
//   val numericalMap: Map[String, Double] =  groupedParams(110)
//     .asInstanceOf[List[Tuple3[String, Byte, Double]]]
//     .map(x => (x._1 -> x._3)).toMap
//   val categoricalMap: Map[String, String] = groupedParams(99)
//     .asInstanceOf[List[Tuple3[String, Byte, String]]]
//     .map(x => (x._1 -> x._3)).toMap
//   val typeMap: Map[String, Byte] = params.map(x => (x._1, x._2)).toMap
//
//   def select(column: String): Either[String, Double] = {
//     typeMap(column) match {
//       case 'c' => Left(categoricalMap(column))
//       case 'n' => Right(numericalMap(column))
//     }
//   }

  // val numericalMap: Map[String, Double] = Map()
  // val categoricalMap: Map[String, String] = Map()
  //
  //
  // val numericMap: Map[String, Double] = Map()
// }
package row

import datavalue._

case class Row(val paramMap: Map[String, DataValue]){
  def apply[T](column: String): T = paramMap(column).asInstanceOf[T]

  def select(column: String): DataValue = paramMap(column)
  def select(columns: String*): Row = Row(paramMap filterKeys columns.toSet)
  def select(columns: Set[String]): Row = Row(paramMap filterKeys columns)
  def select(columns: Iterable[String]): Row = Row(paramMap filterKeys columns.toSet)

  def +(kvPair: Tuple2[String, DataValue]): Row = Row(paramMap + kvPair)

  def ++(otherRow: Row): Row = Row(paramMap ++ otherRow.paramMap)
}
