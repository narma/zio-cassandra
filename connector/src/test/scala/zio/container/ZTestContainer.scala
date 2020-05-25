package zio.container

import com.dimafeng.testcontainers.CassandraContainer
import org.testcontainers.lifecycle.Startable
import zio._
import zio.test.TestFailure

object ZTestContainer {

  def cassandra: ZLayer[Any, TestFailure[Nothing], Has[CassandraContainer]] =
    managed(CassandraContainer("cassandra:3.11.6")).toLayer
      .mapError(TestFailure.die)

  def managed[T <: Startable](container: T): TaskManaged[T] =
    ZManaged.makeEffect {
      container.start()
      container
    }(_.stop())

  def apply[C: Tag]: RIO[Has[C], C] =
    ZIO.service[C]
}
