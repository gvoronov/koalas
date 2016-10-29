package koalas.datasets.bikesharing

import koalas.dataframe.DataFrame
import koalas.schema._

object BikeSharing {
  val daySchema: Schema = Schema(List(
    CategoricalField("instant"),
    CategoricalField("dteday"),
    CategoricalField("season", "season", (1 to 4).map(_.toString).toSet),
    CategoricalField("yr"),
    CategoricalField("mnth", "mnth", (1 to 12).map(_.toString).toSet),
    CategoricalField("holiday", "holiday", Set("0", "1")),
    CategoricalField("weekday", "weekday", (0 to 6).map(_.toString).toSet),
    CategoricalField("workingday", "workingday", Set("0", "1")),
    CategoricalField("weathersit", "weathersit", (1 to 4).map(_.toString).toSet),
    NumericalField("temp"),
    NumericalField("atemp"),
    NumericalField("hum"),
    NumericalField("windspeed"),
    NumericalField("casual"),
    NumericalField("registered"),
    NumericalField("cnt")
  ))

  val hourSchema: Schema = daySchema.insert(
    5, CategoricalField("hr", "hr", (0 to 23).map(_.toString).toSet))

  // Rename to loadDayDF and loadHourDF
  def getDayDF: DataFrame = DataFrame.fromCSV(
    "/datasets/bikesharing/day.csv", daySchema, readerType="InputStream")

  def getHourDF: DataFrame = DataFrame.fromCSV(
    "/datasets/bikesharing/hour.csv", hourSchema, readerType="InputStream")
  // def makeDF(): DataFrame = {}
}
