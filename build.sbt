import AssemblyKeys._ // put this at the top of the file


organization := "final"

name := "default"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.1"

libraryDependencies += "org.apache.hbase" % "hbase" % "0.94.18"


libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"


libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "1.2.1"


resolvers += "Maven Central Server" at "http://repo1.maven.org/maven2"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.4.0"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.0-rc3"

libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.4.0-rc2"

libraryDependencies += "org.scala-lang" % "scala-parser-combinators" % "2.11.0-M4"


// libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "1.2.0"

javaOptions ++= Seq("-XX:MaxPermSize=1024m", "-Xmx2048m")

seq(sbtassembly.Plugin.assemblySettings: _*)

// org.scalastyle.sbt.ScalastylePlugin.Settings

// net.virtualvoid.sbt.graph.Plugin.graphSettings




assemblySettings


mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    // case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("javax", "xml", xs @ _*) => MergeStrategy.first
    // case PathList("META-INF", "maven", "joda-time", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", "jasper", xs @ _*) => MergeStrategy.first
    // case PathList("org", "joda", "time", xs @ _*) => MergeStrategy.first
    case PathList("javax", "servlet", "jsp", xs @ _*) => MergeStrategy.first
    case x => old(x)
  }
}


