package zio.cassandra.session

import com.datastax.oss.driver.api.core.cql.{ Row, SimpleStatement }
import zio.cassandra.session.cql.codec.Reads
import zio.{ IO, ZIO }

trait ZIOCassandraSpecUtils {

  val keyspace = "tests"

  implicit def toStatement(s: String): SimpleStatement = SimpleStatement.newInstance(s)

  def read[T: Reads](row: Row): IO[Throwable, T] =
    ZIO.attempt(Reads[T].read(row))

  def readOpt[T: Reads](row: Option[Row]): IO[Throwable, Option[T]] =
    ZIO.foreach(row)(read[T](_))

}
