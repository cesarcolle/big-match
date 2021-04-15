import sbt.Tests.Setup

name := "big-match"

version := "0.1"

scalaVersion := "2.12.8"

val sparkVersion = "3.1.0"
val jacksonVersion = "2.8.6"
lazy val spark = Seq(
  "org.apache.spark" % "spark-core_2.12" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.12" % sparkVersion
)

val solr = ("org.apache.solr" % "solr-core" % "8.4.1")
  .exclude("org.codehaus.janino", "commons-compiler")
  .exclude("org.codehaus.janino", "janino")
  .exclude("org.antlr", "antlr4-runtime")
  .exclude("org.apache.hadoop", "hadoop-auth")
  .exclude("org.apache.hadoop", "hadoop-hdfs-client")


lazy val core = (project in file("big-match-core"))
  .settings(
    name := """big-match-core""",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.5" % "test",
      "joda-time" % "joda-time" % "2.10.10",
      solr
    ) ++ spark,
    dependencyOverrides ++= Seq("com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
    ),
    testOptions += Setup(cl =>
      cl.loadClass("org.slf4j.LoggerFactory").
        getMethod("getLogger", cl.loadClass("java.lang.String")).
        invoke(null, "ROOT")
    ),
    resolvers ++= Seq(("maven serverlet" at "http://maven.restlet.com/").withAllowInsecureProtocol(true))
  )

lazy val indexer = (project in file("big-match-indexer"))
  .settings(
    name := "big-match-indexer",
    libraryDependencies ++= Seq(
      "com.criteo.cuttle" %% "timeseries" % "0.12.4"
    )
  )
  .dependsOn(core)