package zio.cassandra.session.cql

import com.datastax.oss.driver.api.core.cql.Row
import shapeless._
import shapeless.labelled.{ field, FieldType }
import zio._
import zio.cassandra.session.cql.Reads.{ instance, Configuration }

import java.util.regex.Pattern
import scala.annotation.nowarn

trait Reads[T] {

  def read(row: Row): Task[T]

}

object Reads extends ReadsInstances with ReadsDerivation with ReadsConfigurationInstances {

  def apply[T](implicit reads: Reads[T]): Reads[T] = reads

  def instance[T](f: Row => Task[T]): Reads[T] = (row: Row) => f(row)

  final case class Configuration(transformFieldNames: String => String) {
    def withSnakeCase: Configuration = copy(transformFieldNames = snakeCaseTransformation)
  }

  implicit val defaultReadsConfiguration: Configuration = defaultConfiguration

}

trait ReadsConversions {

  implicit def readsFromRawReads[T: RawReads]: Reads[T] = instance(readByIndex(_, 0))

  protected def readByIndex[T: RawReads](row: Row, index: Int): Task[T] =
    RawReads[T].read(row.getBytesUnsafe(index), row.protocolVersion()).mapError {
      case RawReads.UnexpectedNullError  => UnexpectedNullValueInColumn(row, index)
      case RawReads.InternalError(cause) => cause
    }

}

trait ReadsInstances extends ReadsConversions {

  implicit val rowReads: Reads[Row] = instance(ZIO.succeed(_))

  implicit def tuple2Reads[T1: RawReads, T2: RawReads]: Reads[(T1, T2)] =
    instance { row =>
      val t1 = readByIndex[T1](row, 0)
      val t2 = readByIndex[T2](row, 1)

      t1.zip(t2)
    }

  implicit def tuple3Reads[T1: RawReads, T2: RawReads, T3: RawReads]: Reads[(T1, T2, T3)] =
    instance { row =>
      val t1 = readByIndex[T1](row, 0)
      val t2 = readByIndex[T2](row, 1)
      val t3 = readByIndex[T3](row, 2)

      t1.zip(t2).zip(t3).map { case t1 <*> t2 <*> t3 => (t1, t2, t3) }
    }

  // todo derive more tuple instance with shapeless ?

}

trait ReadsDerivation {

  implicit val hNilReads: Reads[HNil] = instance(_ => ZIO.succeed(HNil))

  implicit def hConsReads[K <: Symbol, H, T <: HList](implicit
    configuration: Configuration,
    hReads: RawReads[H],
    tReads: Reads[T],
    fieldName: Witness.Aux[K]
  ): Reads[FieldType[K, H] :: T] =
    instance { row =>
      for {
        transformedFieldName <- ZIO.succeed(configuration.transformFieldNames(fieldName.value.name))
        head                 <- hReads
                                  .read(row.getBytesUnsafe(transformedFieldName), row.protocolVersion())
                                  .mapError(toThrowable(_, row, transformedFieldName))
        tail                 <- tReads.read(row)
      } yield field[K](head) :: tail
    }

  implicit def genericReads[T, Repr](implicit
    // compiler is lying, this configuration is actually used for Reads[Repr] derivation
    @nowarn("msg=never used") configuration: Configuration,
    gen: LabelledGeneric.Aux[T, Repr],
    reads: Reads[Repr]
  ): Reads[T] =
    instance(row => reads.read(row).map(gen.from))

  private def toThrowable(error: RawReads.Error, row: Row, fieldName: String): Throwable =
    error match {
      case RawReads.UnexpectedNullError  => UnexpectedNullValueInColumn(row, fieldName)
      case RawReads.InternalError(cause) => cause
    }

}

trait ReadsConfigurationInstances {

  val defaultConfiguration: Configuration = Configuration(identity)

  private val basePattern: Pattern = Pattern.compile("([A-Z]+)([A-Z][a-z])")
  private val swapPattern: Pattern = Pattern.compile("([a-z\\d])([A-Z])")

  val snakeCaseTransformation: String => String = s => {
    val partial = basePattern.matcher(s).replaceAll("$1_$2")
    swapPattern.matcher(partial).replaceAll("$1_$2").toLowerCase
  }

}
