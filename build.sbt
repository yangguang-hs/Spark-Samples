version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.7"
val sparkVersion = "3.2.0"

//lazy val root = (project in file("."))
//  .settings(
//    name := "FirstApp"
//  )
resolvers += "Spark Packages Repo" at "https://repos.spark-packages.org"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion  % "provided"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.last
  case x =>
    val defaultStrategy = (assemblyMergeStrategy in assembly).value
    defaultStrategy(x)
}