package zio.cassandra.session.cql.codec

import com.datastax.oss.driver.api.core.data.UdtValue

/** Deserializer created specifically for UDT values.<br> Note that unlike [[zio.cassandra.session.cql.codec.Reads]],
  * this reader can be (is) recursive, so each instance of [[zio.cassandra.session.cql.codec.UdtReads]] can be seen as
  * an instance of [[zio.cassandra.session.cql.codec.CellReads]], while at the same time it might need
  * [[zio.cassandra.session.cql.codec.CellReads]] instances to work.
  */
trait UdtReads[T] {

  def read(udtValue: UdtValue): T

}

object UdtReads extends UdtReadsInstances1 {

  def apply[T](implicit udtReads: UdtReads[T]): UdtReads[T] = udtReads

  def instance[T](f: UdtValue => T): UdtReads[T] = (udtValue: UdtValue) => f(udtValue)

  final implicit class UdtReadsOps[A](private val reads: UdtReads[A]) extends AnyVal {

    def map[B](f: A => B): UdtReads[B] = instance(udtValue => f(reads.read(udtValue)))

  }

}
