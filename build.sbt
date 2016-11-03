lazy val commonSettings = Seq(
  name := "koalas",
  version := "0.0.1",
  scalaVersion := "2.11.8"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*)

// For scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

// For breeze
libraryDependencies ++= Seq(
  "org.scalanlp" %% "breeze" % "0.11.2",
  "org.scalanlp" %% "breeze-natives" % "0.11.2",
  "org.scalanlp" %% "breeze-viz" % "0.11.2"
)

// For using scalameter for microbenchmarking
resolvers += "Sonatype OSS Snapshots" at
  "https://oss.sonatype.org/content/repositories/snapshots"
libraryDependencies += "com.storm-enroute" %% "scalameter-core" % "0.7"


// For shapeless
// resolvers ++= Seq(
//   Resolver.sonatypeRepo("releases"),
//   Resolver.sonatypeRepo("snapshots")
// )
//
// libraryDependencies ++= Seq(
//   "com.chuusai" %% "shapeless" % "2.3.2",
//   compilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
// )
