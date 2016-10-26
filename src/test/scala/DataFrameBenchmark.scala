import org.scalatest.FunSuite
import org.scalameter.measure

import datasets.bikesharing.BikeSharing
import datavalue._
import dataframe.DataFrame



class DataFrameBenchmarkSuite extends FunSuite {
  val df = BikeSharing.getHourDF

  test("partition bike-sharing dataframe and sum partioned column") {
    val partionTime = measure {
      val (leftDF, rightDF) = df.partition(row => row[NumericalValue]("cnt") >= 162)
    }
    info("took " + partionTime.toString)
    // val cntLeftSum = leftDF.select[NumericalValue]("cnt").reduce((a, b) => a + b)
    // val cntRightSum = rightDF.select[NumericalValue]("cnt").reduce((a, b) => a + b)
    //
    // assert(cntRightSum == 561079)
    // assert(cntLeftSum == 2731600)
  }

  test("sum cnt column on two resulting partitioned data frames") {
    val (leftDF, rightDF) = df.partition(row => row[NumericalValue]("cnt") >= 162)

    val allSumTime = measure {
      val cntAlltSum = df.select[NumericalValue]("cnt").reduce((a, b) => a + b)
    }
    info("total sum took " + allSumTime.toString)

    val leftSumTime = measure {
      val cntLeftSum = leftDF.select[NumericalValue]("cnt").reduce((a, b) => a + b)
    }
    info("left sum took " + leftSumTime.toString)

    val rightSumTime = measure {
      val cntRightSum = rightDF.select[NumericalValue]("cnt").reduce((a, b) => a + b)
    }
    info("right sum took " + rightSumTime.toString)
  }
}
  // val cntLeftSum = leftDF.select[NumericalValue]("cnt").reduce((a, b) => a + b)
  // val cntRightSum = rightDF.select[NumericalValue]("cnt").reduce((a, b) => a + b)
  //
  // assert(cntRightSum == 561079)
  // assert(cntLeftSum == 2731600)

// import datavalue._

// object DataFrameBenchmark extends Bench.LocalTime {
//   // val sizes = Gen.range("size")(300000, 1500000, 300000)
//   //
//   // val ranges = for {
//   //   size <- sizes
//   // } yield 0 until size
//   val df = BikeSharing.getHourDF
//   val dfGen = Gen.unit("asdf")
//
//   performance of "DataFrame" in {
//     measure method "partition" in {
//       using(dfGen) in {
//         // using(ranges) in {
//           // r => r.map(_ + 1)
//         def ()
//         dfGen =>
//         val (leftDF, rightDF) = df.partition(row => row[NumericalValue]("cnt") >= 162)
//         ()
//         // }
//       }
//     }
//   }
// }
//
// object RangeBenchmark
// extends Bench.LocalTime {
//   val sizes = Gen.range("size")(300000, 1500000, 300000)
//
//   val ranges = for {
//     size <- sizes
//   } yield 0 until size
//
//   performance of "Range" in {
//     measure method "map" in {
//       using(ranges) in {
//         r => r.map(_ + 1)
//       }
//     }
//   }
// }
