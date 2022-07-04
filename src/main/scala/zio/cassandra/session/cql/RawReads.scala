package zio.cassandra.session.cql

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry
import com.datastax.oss.driver.api.core.`type`.reflect.GenericType
import zio.{ IO, Task, ZIO }

import java.nio.ByteBuffer
import java.time.{ Instant, LocalDate, LocalTime }
import java.util.UUID
import scala.collection.Factory
import scala.jdk.CollectionConverters.{ IterableHasAsScala, MapHasAsScala, SetHasAsScala }
import scala.reflect.{ classTag, ClassTag }

/** Low-level alternative for [[com.datastax.oss.driver.api.core.type.codec.TypeCodec]] that is meant ot be resolved at
  * a compile-time
  */
trait RawReads[+T] {

  def read(bytes: ByteBuffer, protocol: ProtocolVersion): IO[RawReads.Error, T]

}

object RawReads {

  sealed trait Error

  final case object UnexpectedNullError extends Error

  final case class InternalError(cause: Throwable) extends Error

  def apply[T](implicit reads: RawReads[T]): RawReads[T] = reads

  def instance[T](f: (ByteBuffer, ProtocolVersion) => IO[RawReads.Error, T]): RawReads[T] =
    (bytes: ByteBuffer, protocol: ProtocolVersion) => f(bytes, protocol)

  implicit class RawReadsOps[A](private val reads: RawReads[A]) extends AnyVal {
    def map[B](f: A => B): RawReads[B] = instance((b, p) => reads.read(b, p).map(f))
  }

  implicit val stringRawReads: RawReads[String] = forClassFromCodecUnsafe[String]

  implicit val booleanRawReads: RawReads[Boolean] = forClassFromCodecUnsafe[java.lang.Boolean].map(r => r)

  implicit val shortRawReads: RawReads[Short]   = forClassFromCodecUnsafe[java.lang.Short].map(r => r)
  implicit val intRawReads: RawReads[Int]       = forClassFromCodecUnsafe[java.lang.Integer].map(r => r)
  implicit val longRawReads: RawReads[Long]     = forClassFromCodecUnsafe[java.lang.Long].map(r => r)
  implicit val bigIntRawReads: RawReads[BigInt] = forClassFromCodecUnsafe[java.math.BigInteger].map(r => r)

  implicit val doubleRawReads: RawReads[Double]         = forClassFromCodecUnsafe[java.lang.Double].map(r => r)
  implicit val bigDecimalRawReads: RawReads[BigDecimal] = forClassFromCodecUnsafe[java.math.BigDecimal].map(r => r)

  implicit val localDateRawReads: RawReads[LocalDate] = forClassFromCodecUnsafe[LocalDate]
  implicit val localTimeRawReads: RawReads[LocalTime] = forClassFromCodecUnsafe[LocalTime]
  implicit val instantRawReads: RawReads[Instant]     = forClassFromCodecUnsafe[Instant]

  implicit val uuidRawReads: RawReads[UUID] = forClassFromCodecUnsafe[UUID]

  implicit val byteBufferRawReads: RawReads[ByteBuffer] = forClassFromCodecUnsafe[ByteBuffer]

  implicit def optionRawReads[T: RawReads]: RawReads[Option[T]] =
    instance((bytes, protocol) => ZIO.foreach(Option(bytes))(RawReads[T].read(_, protocol)))

  implicit def iterableRawReads[T: RawReads, Coll[_] <: Iterable[_]](implicit
    f: Factory[T, Coll[T]]
  ): RawReads[Coll[T]] = {
    val cachedCodec = CodecRegistry.DEFAULT.codecFor(GenericType.listOf(classOf[ByteBuffer]))
    instance { (bytes, protocol) =>
      ZIO.foreach(cachedCodec.decode(bytes, protocol).asScala)(RawReads[T].read(_, protocol)).map(f.fromSpecific)
    }
  }

  implicit def setRawReads[T: RawReads]: RawReads[Set[T]] = {
    val cachedCodec = CodecRegistry.DEFAULT.codecFor(GenericType.setOf(classOf[ByteBuffer]))
    instance { (bytes, protocol) =>
      for {
        _          <- checkNull(bytes)
        setOfBytes <- Task(cachedCodec.decode(bytes, protocol).asScala.toSet).mapError(InternalError)
        set        <- ZIO.foreach(setOfBytes)(RawReads[T].read(_, protocol))
      } yield set
    }
  }

  implicit def mapRawReads[K: RawReads, V: RawReads]: RawReads[Map[K, V]] = {
    val cachedCodec = CodecRegistry.DEFAULT.codecFor(GenericType.mapOf(classOf[ByteBuffer], classOf[ByteBuffer]))
    instance { (bytes, protocol) =>
      ZIO.foreach(cachedCodec.decode(bytes, protocol).asScala.toMap) { (key, value) =>
        for {
          k <- RawReads[K].read(key, protocol)
          v <- RawReads[V].read(value, protocol)
        } yield (k, v)
      }
    }
  }

  def checkNull(bytes: ByteBuffer): IO[UnexpectedNullError.type, Unit] =
    ZIO.fail(UnexpectedNullError).when(bytes == null)

  private def forClassFromCodecUnsafe[T: ClassTag]: RawReads[T] = {
    // might throw an exception, but we'd rather immediately die in this case
    // do not inline it, otherwise this search will happen each time when reads is needed
    val cachedCodec = CodecRegistry.DEFAULT.codecFor(classTag[T].runtimeClass.asInstanceOf[Class[T]])
    instance { (bytes, protocol) =>
      checkNull(bytes) *> Task(cachedCodec.decode(bytes, protocol)).mapError(InternalError)
    }
  }

}
