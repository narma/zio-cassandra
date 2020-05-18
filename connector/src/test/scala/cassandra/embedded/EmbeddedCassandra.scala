package cassandra.embedded

import java.net.InetAddress

import com.github.nosan.embedded.cassandra.EmbeddedCassandraFactory
import com.github.nosan.embedded.cassandra.api.Cassandra
import com.github.nosan.embedded.cassandra.artifact.Artifact
import zio.{Has, Task, ZLayer}

object EmbeddedCassandra {

  type Embedded = Has[Cassandra]

  def createInstance(port: Int): ZLayer[Any, Throwable, Embedded] =
    Task {
      val embeddedFactory = new EmbeddedCassandraFactory()
      embeddedFactory.setPort(port)

      // only > 4.x cassandra supports jdk 11 or later
      embeddedFactory.setArtifact(Artifact.ofVersion("4.0-alpha4"))
      embeddedFactory.setAddress(InetAddress.getLoopbackAddress)
      embeddedFactory.setName("test_cassandra")
      val cassandra = embeddedFactory.create()
      cassandra.start()
      cassandra
    }.toManaged(c => Task(c.stop()).orDie).toLayer

}
