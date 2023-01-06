import sbt._

object Dependencies {

  object V {
    val scala2   = "2.13.10"
    val scala3   = "3.2.1"
    val allScala = Seq(scala2, scala3)

    val cassandraDriverVersion = "4.15.0"
  }

  import V._

  val zioVersion = "2.0.5"

  val testContainersVersion = "0.40.11"

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

  val cache = Seq(
    "com.github.ben-manes.caffeine" % "caffeine" % "3.1.2"
  )


  val testIntegrationDeps = Seq(
    "org.wvlet.airframe" %% "airframe-log"                   % "22.11.0",
    "org.slf4j"           % "slf4j-jdk14"                    % "2.0.3",
    "com.dimafeng"       %% "testcontainers-scala-core"      % testContainersVersion,
    "com.dimafeng"       %% "testcontainers-scala-cassandra" % testContainersVersion,
    "org.testcontainers"  % "testcontainers"                 % "1.17.4"
  ).map(_ % "it")

}
