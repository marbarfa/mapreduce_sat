
name := "mapreduce-3sat"

version := "1.0"

def scalaCompilerVersion = "2.11.2"
def hadoopVersion = "2.4.0"
def hbaseVersion = "0.98.6.1-hadoop2"

scalaVersion := scalaCompilerVersion
compileOrder := CompileOrder.Mixed

//autoScalaLibrary := false
exportJars := true
//SCALA
libraryDependencies += "org.scala-lang" % "scala-compiler" % scalaCompilerVersion
libraryDependencies += "org.scala-lang" % "scala-library" % scalaCompilerVersion
libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaCompilerVersion

//HADOOP
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % Compile,
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % Compile,
  "log4j" % "log4j" % "1.2.17" % Compile,
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion % Compile,
  "org.apache.hadoop" % "hadoop-mapreduce-client-app" % hadoopVersion % Compile,
  "org.apache.hadoop" % "hadoop-mapreduce-client-common" % hadoopVersion % Compile,
  "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion % Compile,
  "org.apache.hadoop" % "hadoop-yarn-common" % hadoopVersion % Compile
)

//HBASE
libraryDependencies ++= Seq(
  "org.apache.hbase" % "hbase-client" % hbaseVersion % Compile,
  "org.apache.hbase" % "hbase-it" % hbaseVersion % Compile,
  "org.apache.hbase" % "hbase-common" % hbaseVersion % Compile,
  "org.apache.hbase" % "hbase-server" % hbaseVersion % Compile,
  "org.apache.hbase" % "hbase-prefix-tree" % hbaseVersion % Compile,
  "org.apache.hbase" % "hbase-thrift" % hbaseVersion % Compile,
  "org.apache.hbase" % "hbase-protocol" % hbaseVersion % Compile,
  "org.apache.hbase" % "hbase-hadoop2-compat" % hbaseVersion % Compile,
  "org.htrace" % "htrace-core" % "3.0" % Compile,
  "org.apache.zookeeper" % "zookeeper" % "3.4.5" % Compile
)