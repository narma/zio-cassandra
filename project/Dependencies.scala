import sbt._

object Dependencies {

  val cassandraDriverVersion = "4.14.0"

  val zioVersion = "1.0.13"

  val testContainersVersion = "0.40.2"

  val cassandraDependencies = Seq(
    "com.datastax.oss" % "java-driver-core" % cassandraDriverVersion
  )

  val commonDependencies = Seq(
    "com.chuusai" %% "shapeless" % "2.3.8"
  )

  val zioDependencies = Seq(
    "dev.zio" %% "zio"         % zioVersion,
    "dev.zio" %% "zio-streams" % zioVersion
  )

  val testCommon = Seq(
    "dev.zio" %% "zio-test"     % zioVersion,
    "dev.zio" %% "zio-test-sbt" % zioVersion
  ).map(_ % "it,test")

  val testIntegrationDeps = Seq(
    "org.wvlet.airframe" %% "airframe-log"                   % "20.5.1",
    "org.slf4j"           % "slf4j-jdk14"                    % "1.7.32",
    "com.dimafeng"       %% "testcontainers-scala-core"      % testContainersVersion,
    "com.dimafeng"       %% "testcontainers-scala-cassandra" % testContainersVersion,
    "org.testcontainers"  % "testcontainers"                 % "1.16.3"
  ).map(_ % "it")

}
