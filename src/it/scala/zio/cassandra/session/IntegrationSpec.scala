package zio.cassandra.session

import wvlet.log.{ LogFormatter, LogLevel, LogSupport, Logger }
import zio.cassandra.session.cql.CqlSpec
import zio.cassandra.session.cql.codec.{ ReadsSpec, UdtReadsSpec, UdtWritesSpec }
import zio.container.TestsSharedInstances
import zio.test.DefaultRunnableSpec

object IntegrationSpec extends DefaultRunnableSpec with TestsSharedInstances with LogSupport {

  Logger.setDefaultLogLevel(LogLevel.INFO)
  Logger.setDefaultFormatter(LogFormatter.SourceCodeLogFormatter)
  Logger.scanLogLevels

  override def spec =
    suite("zio-cassandra")(
      SessionSpec.sessionTests,
      CqlSpec.cqlSuite,
      ReadsSpec.readsTests,
      UdtReadsSpec.readsTests,
      UdtWritesSpec.writesTests
    )
      .provideCustomLayerShared(layer)
}
