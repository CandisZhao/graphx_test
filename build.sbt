name := "graphx_test"

version := "1.0"

scalaVersion := "2.11.8"



libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "2.7.3",
  "org.apache.hbase" % "hbase-common" % "1.2.2",
  "org.apache.hbase" % "hbase-client" % "1.2.2",
  "org.apache.hbase" % "hbase-server" % "1.2.2"


)


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.0",
  "org.apache.spark" %% "spark-sql" % "2.0.0",
  "org.apache.spark" %% "spark-graphx" % "2.0.0"
)