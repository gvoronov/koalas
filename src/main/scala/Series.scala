package series

class Series[T](val values: Vector[T]){
  def apply(index: Int): T = values(index)

  def map[R](f: (T) => R): Vector[R] = values.map(f)
  def reduce[R >: T](op: (R, R) => R): R = values.reduce(op)
  def filter(p: (T) => Boolean): Vector[T] = values.filter(p)
  def groupBy[R](f: (T) => R): Map[R, Vector[T]] = values.groupBy(f)
}

object Series{
  def apply[T](values: Vector[T]): Series[T] = new Series[T](values)
}
