lazy val commonSettings = Seq(
  name := "CSVReader",
  version := "0.0.1",
  scalaVersion := "2.10.6"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*)

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.0" % "test"
