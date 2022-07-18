package zio.cassandra.session

import com.datastax.oss.driver.api.core.cql.{ Row, SimpleStatement }
import zio.{ IO, Task, ZIO }
import zio.cassandra.session.cql.codec.Reads
import zio.test.TestFailure

trait CassandraSpecUtils {

  val keyspace = "tests"

  implicit def toStatement(s: String): SimpleStatement = SimpleStatement.newInstance(s)

  def read[T: Reads](row: Row): IO[TestFailure[Throwable], T] =
    Task(Reads[T].read(row)).mapError(TestFailure.die)

  def readOpt[T: Reads](row: Option[Row]): IO[TestFailure[Throwable], Option[T]] =
    ZIO.foreach(row)(read[T](_))

}
