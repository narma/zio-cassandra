package cassandra.embedded

import java.net.InetAddress

import com.github.nosan.embedded.cassandra.EmbeddedCassandraFactory
import com.github.nosan.embedded.cassandra.api.Cassandra
import zio.{Has, ZIO, ZLayer}

object EmbeddedCassandra {

  type Embedded = Has[Cassandra]

  def createInstance(port: Int): ZLayer[Any, Throwable, Embedded] =
    ZLayer.fromEffect(ZIO {
      val embeddedFactory = new EmbeddedCassandraFactory()
      embeddedFactory.setPort(port)
      embeddedFactory.setAddress(InetAddress.getLocalHost)
      embeddedFactory.setName("test_cassandra")
      val cassandra = embeddedFactory.create()
      cassandra.start()
      cassandra
    })

}
