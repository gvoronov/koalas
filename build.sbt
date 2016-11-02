lazy val commonSettings = Seq(
  name := "koalas",
  version := "0.0.1",
  scalaVersion := "2.11.8"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

libraryDependencies ++= Seq(
  "org.scalanlp" %% "breeze" % "0.11.2",
  "org.scalanlp" %% "breeze-natives" % "0.11.2",
  "org.scalanlp" %% "breeze-viz" % "0.11.2"
)

// For using scalameter for microbenchmarking
resolvers += "Sonatype OSS Snapshots" at
  "https://oss.sonatype.org/content/repositories/snapshots"
libraryDependencies += "com.storm-enroute" %% "scalameter-core" % "0.7"

// testFrameworks += new TestFramework(
//   "org.scalameter.ScalaMeterFramework")
// logBuffered := false
// parallelExecution in Test := false
