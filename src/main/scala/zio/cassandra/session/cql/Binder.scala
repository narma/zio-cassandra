package zio.cassandra.session.cql

//import com.datastax.oss.driver.api.core.`type`.UserDefinedType
import com.datastax.oss.driver.api.core.data.{SettableByIndex, UdtValue}
import shapeless.{::, HList, HNil, Widen}

import java.nio.ByteBuffer
import java.time.{Instant, LocalDate}
import java.util.UUID
import scala.annotation.implicitNotFound

@implicitNotFound("""Cannot find or construct a Binder instance for type:

  ${T}

  Construct it if needed, please refer to Binder source code for guidance
""")
trait Binder[T] { self =>
  def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: T): S

  def nextIndex(index: Int): Int = index + 1

  def contramap[U](f: U => T): Binder[U] = new Binder[U] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: U): S =
      self.bind(statement, index, f(value))
  }
}

object Binder extends BinderLowerPriority with BinderLowestPriority {

  def apply[T](implicit binder: Binder[T]): Binder[T] = binder

  implicit val stringBinder: Binder[String] = new Binder[String] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: String): S =
      statement.setString(index, value)
  }

  implicit val doubleBinder: Binder[Double] = new Binder[Double] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: Double): S =
      statement.setDouble(index, value)
  }

  implicit val intBinder: Binder[Int] = new Binder[Int] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: Int): S =
      statement.setInt(index, value)
  }

  implicit val longBinder: Binder[Long] = new Binder[Long] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: Long): S =
      statement.setLong(index, value)
  }

  implicit val byteBufferBinder: Binder[ByteBuffer] = new Binder[ByteBuffer] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: ByteBuffer): S =
      statement.setByteBuffer(index, value)
  }

  implicit val localDateBinder: Binder[LocalDate] = new Binder[LocalDate] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: LocalDate): S =
      statement.setLocalDate(index, value)
  }

  implicit val instantBinder: Binder[Instant] = new Binder[Instant] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: Instant): S =
      statement.setInstant(index, value)
  }

  implicit val booleanBinder: Binder[Boolean] = new Binder[Boolean] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: Boolean): S =
      statement.setBoolean(index, value)
  }

  implicit val uuidBinder: Binder[UUID] = new Binder[UUID] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: UUID): S =
      statement.setUuid(index, value)
  }

  implicit val bigIntBinder: Binder[BigInt] = new Binder[BigInt] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: BigInt): S =
      statement.setBigInteger(index, value.bigInteger)
  }

  implicit val bigDecimalBinder: Binder[BigDecimal] = new Binder[BigDecimal] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: BigDecimal): S =
      statement.setBigDecimal(index, value.bigDecimal)
  }

  implicit val shortBinder: Binder[Short] = new Binder[Short] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: Short): S =
      statement.setShort(index, value)
  }

  implicit val userDefinedTypeValueBinder: Binder[UdtValue] = new Binder[UdtValue] {
      override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: UdtValue): S =
        statement.setUdtValue(index, value)
    }

  implicit def optionBinder[T: Binder]: Binder[Option[T]] = new Binder[Option[T]] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: Option[T]): S= value match {
      case Some(x) => Binder[T].bind(statement, index, x)
      case None    => statement.setToNull(index)
    }
  }


  implicit def widenBinder[T: Binder, X <: T](implicit wd: Widen.Aux[X, T]): Binder[X] = new Binder[X] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: X): S =
      Binder[T].bind(statement, index, wd.apply(value))
  }

}

trait BinderLowerPriority {

  /** This typeclass instance is used to (inductively) derive datatypes that can have arbitrary amounts of nesting
    * @param ev
    *   is evidence that a typeclass instance of CassandraTypeMapper exists for A
    * @tparam A
    *   is the Scala datatype that needs to be written to Cassandra
    * @return
    */
  implicit def deriveBinderFromCassandraTypeMapper[A](implicit ev: CassandraTypeMapper[A]): Binder[A] = new Binder[A] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: A): S =  {
      val datatype  = statement.getType(index)
      val cassandra = ev.toCassandra(value, datatype)
      statement.set(index, cassandra, ev.classType)
    }
  }
}

trait BinderLowestPriority {
  implicit val hNilBinder: Binder[HNil]                                   = new Binder[HNil] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: HNil): S = statement
  }
  implicit def hConsBinder[H: Binder, T <: HList: Binder]: Binder[H :: T] = new Binder[H :: T] {
    override def bind[S <: SettableByIndex[S]](statement: S, index: Int, value: H :: T): S = {
      val applied = Binder[H].bind(statement, index, value.head)
      val nextIndex = Binder[H].nextIndex(index)
      Binder[T].bind(applied, nextIndex, value.tail)
    }
  }
}
