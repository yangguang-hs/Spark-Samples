# Spark-Samples
This is the sample project to run spark.

With some simple schema. 

It is using spark 3.2.0 and scala 2.13.7 and java 11


To enable run in intellij:
1. add % "provided" in libraryDependencies
2. add resolvers += "Spark Packages Repo" at "https://repos.spark-packages.org"
3. in the run config, 
   1. add `application`
   2. check `add dependencies with 'provided' scope to classpath`


to run with sbt assembly:
1. sbt assembly
2. spark-submit --class {mainclass} {target/jar location} {arguments}
   1. spark-submit --class First target/firstApp.jap
