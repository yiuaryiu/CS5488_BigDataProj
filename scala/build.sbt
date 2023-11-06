ThisBuild / version := "1.0"

//ThisBuild / scalaVersion := "2.12.15"
ThisBuild / scalaVersion := "2.11.1"

libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-mllib" % "3.1.2",
//  "org.apache.spark" %% "spark-core" % "3.1.2",
//  "org.apache.spark" %% "spark-sql" % "3.1.2",
//  "org.scalanlp" % "nak" % "1.2.1"
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  "org.scalanlp" % "nak_2.11" % "1.3",
)


