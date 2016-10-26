lazy val commonSettings = Seq(
  name := "koalas",
  version := "0.0.1",
  scalaVersion := "2.11.8"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*)

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.0" % "test"

libraryDependencies ++= Seq(
  "org.scalanlp" %% "breeze" % "0.11.2",
  "org.scalanlp" %% "breeze-natives" % "0.11.2",
  "org.scalanlp" %% "breeze-viz" % "0.11.2"
)

resolvers += "Sonatype OSS Snapshots" at
  "https://oss.sonatype.org/content/repositories/snapshots"

// For using scalameter for microbenchmarking
libraryDependencies += "com.storm-enroute" %% "scalameter" % "0.7"
testFrameworks += new TestFramework(
  "org.scalameter.ScalaMeterFramework")
logBuffered := false
parallelExecution in Test := false
