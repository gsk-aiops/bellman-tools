name := "bellman-tools"

version := "0.1"

scalaVersion := "2.11.12"
val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "com.github.gsk-aiops" %% "bellman-spark-engine" % "0.1.12+680-9c884959-SNAPSHOT",
  "net.sansa-stack" % "sansa-rdf-spark_2.11" % "0.7.1",
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.scalatest" % "scalatest_2.11" % "2.2.6" % Test
)

dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
  )
}

resolvers += "oss-sonatype" at "https://oss.sonatype.org/content/repositories/snapshots/"

assemblyMergeStrategy in assembly := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVersion + "_" + module.revision + "." + artifact.extension
}