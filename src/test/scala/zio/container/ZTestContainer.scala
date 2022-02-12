package zio.container

import com.dimafeng.testcontainers.CassandraContainer
import org.testcontainers.lifecycle.Startable
import org.testcontainers.utility.DockerImageName

import zio._
import zio.blocking.{ effectBlocking, Blocking }
import zio.test.TestFailure

object ZTestContainer {

  def cassandra: ZLayer[Blocking, TestFailure[Nothing], Has[CassandraContainer]] =
    managed(CassandraContainer(dockerImageNameOverride = DockerImageName.parse("cassandra:3.11.11")))
      .mapError(TestFailure.die)
      .toLayer

  def managed[T <: Startable](container: T): RManaged[Blocking, T] =
    ZManaged.make {
      effectBlocking {
        container.start()
        container
      }
    }(c => effectBlocking(c.stop()).orDie)

  def apply[C: Tag]: RIO[Has[C], C] =
    ZIO.service[C]
}
