package zio.cassandra.session

import com.dimafeng.testcontainers.CassandraContainer
import wvlet.log.{ LogFormatter, LogLevel, LogSupport, Logger }
import zio.container.Layers
import zio.test.{ Spec, TestEnvironment, ZIOSpec }
import zio.{ Scope, ZLayer }

abstract class ZIOCassandraSpec extends ZIOSpec[CassandraContainer with Session] with LogSupport {

  Logger.setDefaultLogLevel(LogLevel.INFO)
  Logger.setDefaultFormatter(LogFormatter.SourceCodeLogFormatter)
  Logger.scanLogLevels

  override val bootstrap: ZLayer[Any, Any, Environment] = Layers.layer

  def spec: Spec[Environment with TestEnvironment with Scope, Any]

}
