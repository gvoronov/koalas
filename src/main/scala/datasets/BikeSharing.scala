package koalas.datasets.bikesharing

import koalas.dataframe.DataFrame
import koalas.schema._

object BikeSharing {
  val daySchema: Schema = Schema(List(
    CategoricalField("instant"),
    CategoricalField("dteday"),
    CategoricalField("season", Some("season"), (1 to 4).map(_.toString).toSet),
    CategoricalField("yr"),
    CategoricalField("mnth", Some("mnth"), (1 to 12).map(_.toString).toSet),
    CategoricalField("holiday", Some("holiday"), Set("0", "1")),
    CategoricalField("weekday", Some("weekday"), (0 to 6).map(_.toString).toSet),
    CategoricalField("workingday", Some("workingday"), Set("0", "1")),
    CategoricalField("weathersit", Some("weathersit"), (1 to 4).map(_.toString).toSet),
    NumericalField("temp"),
    NumericalField("atemp"),
    NumericalField("hum"),
    NumericalField("windspeed"),
    NumericalField("casual"),
    NumericalField("registered"),
    NumericalField("cnt")
  ))

  val hourSchema: Schema = daySchema.insert(
    5, CategoricalField("hr", Some("hr"), (0 to 23).map(_.toString).toSet))

  // Rename to loadDayDF and loadHourDF
  def loadDayDF: DataFrame = DataFrame.fromCSV(
    "/koalas/datasets/bikesharing/day.csv", daySchema, readerType="InputStream")

  def loadHourDF: DataFrame = DataFrame.fromCSV(
    "/koalas/datasets/bikesharing/hour.csv", hourSchema, readerType="InputStream")
  // def makeDF(): DataFrame = {}
}
