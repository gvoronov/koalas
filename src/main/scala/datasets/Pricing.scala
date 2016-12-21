package koalas.datasets.pricing

import breeze.stats.distributions._
import breeze.linalg._
import breeze.numerics._

import koalas.dataframe.DataFrame
import koalas.row.Row
import koalas.datavalue._

object Pricing {
  /**
   * [priceBounds description]
   * @param priceBounds
   * @param storeFactors
   * @param weekFactors
   * @param gamma
   * @param epsilon
   */
  def makeDF(
      priceBounds: List[Tuple2[Double, Double]], storeFactors: List[Double],
      weekFactors: List[Double], gamma: DenseMatrix[Double], epsilon: Double): DataFrame = {
    val numItems = priceBounds.length
    val priceGenerators = priceBounds.map(bounds => Uniform(bounds._1, bounds._2))
    val normalError = Gaussian(0, epsilon)

    var df = DataFrame.empty
    for ((storeFactor, r) <- storeFactors.zipWithIndex) {
      for ((weekFactor, t) <- weekFactors.zipWithIndex) {
        val row = Row(Map(
          "week" -> CategoricalValue(t.toString),
          "store" -> CategoricalValue(r.toString)
        ))

        val p = DenseVector(priceGenerators.map(_.draw).toArray).map(p => rint(p * 100) / 100)
        val logp = log(p)
        val logq = gamma * log(p) + storeFactor + weekFactor + normalError.samplesVector(numItems)
        val q = floor(exp(logq))

        val ps = Row(p.toArray.toVector.zipWithIndex
          .map(pair => "p_" + pair._2.toString -> NumericalValue(pair._1)).toMap)
        val qs = Row(q.toArray.toVector.zipWithIndex
          .map(pair => "q_" + pair._2.toString -> NumericalValue(pair._1)).toMap)

        df = df + (row + ps + qs)
      }
    }
    df
  }

  /**
   * [numItems description]
   * @param numItems
   * @param lowerPriceBound
   * @param upperPriceBound
   * @param numWeeks
   * @param weekWeight
   * @param numStores
   * @param storeWeight
   * @param gammaOwnMean
   * @param gammaOwnVariance
   * @param gammaCrossMean
   * @param gammaCrossVariance
   * @param epsilon
   */
  def makeDF(
      numItems: Int = 5, lowerPriceBound: Double = 1.50, upperPriceBound: Double = 2.00,
      numWeeks: Int = 52, weekWeight: Double = 2.0, numStores: Int = 100,
      storeWeight: Double = 1.0, gammaOwnMean: Double = -2.0, gammaOwnVariance: Double = 0.1,
      gammaCrossMean: Double = 1.0, gammaCrossVariance: Double = 0.1,
      epsilon: Double = 0.005): DataFrame = {
    val priceBounds: List[Tuple2[Double, Double]] =
      (0 until numItems).map(_ => (lowerPriceBound, upperPriceBound)).toList

    val storeFactors: List[Double] =
      (randomDouble(numStores) * storeWeight).toArray.toList
    val weekFactors: List[Double] =
      (sin(linspace(0, constants.Pi, numWeeks)) * weekWeight).toArray.toList

    val gammaOwn = Gaussian(gammaOwnMean, gammaOwnVariance).samplesVector(numItems)
    val gammaCross = Gaussian(gammaCrossMean, gammaCrossVariance).samplesVector(numItems*numItems)
      .toDenseMatrix.reshape(numItems, numItems)
    val gamma: DenseMatrix[Double] = diag(gammaOwn) + gammaCross - diag(diag(gammaCross))

    makeDF(priceBounds, storeFactors, weekFactors, gamma, epsilon)
  }
}
