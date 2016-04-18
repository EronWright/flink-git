
name := "flink-git"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= {

  Seq(
    "org.apache.flink" %% "flink-streaming-scala" % "1.0.1",
    "org.apache.flink" %% "flink-streaming-java" % "1.0.1" % "test" classifier "tests",
    "org.apache.flink" % "flink-core" % "1.0.1" % "test" classifier "tests",
    "org.apache.flink" %% "flink-tests" % "1.0.1" % "test" classifier "tests",
    "org.apache.flink" %% "flink-test-utils" % "1.0.1" % "test",
    "org.apache.flink" %% "flink-runtime" % "1.0.1" % "test" classifier "tests",
    "org.eclipse.jgit" % "org.eclipse.jgit" % "4.3.0.201604071810-r",
    "junit" % "junit" % "4.11" % Test
  )
}