scalaVersion := "2.11.7"

name := "HiveScheduler"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "1.5.1"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.5.1"
