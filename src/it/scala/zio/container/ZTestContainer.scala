package zio.container

import com.dimafeng.testcontainers.CassandraContainer
import org.testcontainers.lifecycle.Startable
import org.testcontainers.utility.DockerImageName
import zio._

object ZTestContainer {

  val cassandra: ZLayer[Any, Throwable, CassandraContainer] =
    ZLayer.scoped {
      scoped(CassandraContainer(dockerImageNameOverride = DockerImageName.parse("cassandra:4.0.3")))
    }

  def scoped[T <: Startable](container: => T): RIO[Scope, T] =
    ZIO.acquireRelease {
      ZIO.succeed(container).flatMap { container =>
        ZIO.attemptBlocking {
          container.start()
          container
        }
      }
    } { container =>
      ZIO.attemptBlocking(container.stop()).orDie
    }
}
