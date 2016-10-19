import scala.collection.immutable.LinearSeq

// class DataFrame extends LinearSeq[Row]
class DataFrame(val rows: LinearSeq[Row]) extends LinearSeq[Row] {
  def apply(idx: Int): Row = rows(idx)
  def length: Int = rows.length
}
//
// // object DataFrame {
// //   def apply() = new DataFrame
// // }
