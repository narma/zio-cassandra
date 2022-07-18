import sbt._

object Dependencies {

  object V {
    val scala2   = "2.13.8"
    val scala3   = "3.1.3"
    val allScala = Seq(scala2, scala3)
  }

  val cassandraDriverVersion = "4.14.1"

  val zioVersion = "1.0.16"

  val testContainersVersion = "0.40.8"

  val cassandraDependencies = Seq(
    "com.datastax.oss" % "java-driver-core" % cassandraDriverVersion % "provided"
  )

  val zioDependencies = Seq(
    "dev.zio" %% "zio"         % zioVersion,
    "dev.zio" %% "zio-macros"  % zioVersion % "compile-internal",
    "dev.zio" %% "zio-streams" % zioVersion
  )

  val testCommon = Seq(
    "dev.zio" %% "zio-test"     % zioVersion,
    "dev.zio" %% "zio-test-sbt" % zioVersion
  ).map(_ % "it,test")

  val testIntegrationDeps = Seq(
    "org.wvlet.airframe" %% "airframe-log"                   % "22.6.1",
    "org.slf4j"           % "slf4j-jdk14"                    % "1.7.36",
    "com.dimafeng"       %% "testcontainers-scala-core"      % testContainersVersion,
    "com.dimafeng"       %% "testcontainers-scala-cassandra" % testContainersVersion,
    "org.testcontainers"  % "testcontainers"                 % "1.17.2"
  ).map(_ % "it")

}
