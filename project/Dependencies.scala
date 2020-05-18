import sbt._

object Dependencies {

  val cassandraDriverVersion = "4.6.1"

  val zioVersion = "1.0.0-RC19"

  val zioConfigVersion = "1.0.0-RC18"

  val javaStreamsInterop = "1.0.3.5-RC8"

  val cassandraDependencies = Seq(
    "com.datastax.oss" % "java-driver-core" % cassandraDriverVersion
  )

  val zioDependencies = Seq(
    "dev.zio" %% "zio"                         % zioVersion,
    "dev.zio" %% "zio-streams"                 % zioVersion,
    "dev.zio" %% "zio-interop-reactivestreams" % javaStreamsInterop,
    "dev.zio" %% "zio-config"                  % zioConfigVersion,
    "dev.zio" %% "zio-config-magnolia"         % zioConfigVersion,
    "dev.zio" %% "zio-config-typesafe"         % zioConfigVersion
  )

  val testCommon = Seq(
    "com.github.nosan"   % "embedded-cassandra" % "3.0.3"    % Test,
    "org.wvlet.airframe" %% "airframe-log"      % "20.5.1"   % Test,
    "org.slf4j"          % "slf4j-jdk14"        % "1.7.21"   % Test,
    "dev.zio"            %% "zio-test"          % zioVersion % Test,
    "dev.zio"            %% "zio-test-sbt"      % zioVersion % Test
  )

}
