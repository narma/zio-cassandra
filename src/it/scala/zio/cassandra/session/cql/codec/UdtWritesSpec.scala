package zio.cassandra.session.cql.codec

import zio.cassandra.session.cql.CqlStringContext
import zio.cassandra.session.cql.unsafe.lift
import zio.cassandra.session.{ Session, ZIOCassandraSpec, ZIOCassandraSpecUtils }
import zio.test._

object UdtWritesSpec extends ZIOCassandraSpec with ZIOCassandraSpecUtils {

  final case class Udt(ALLUPPER: String, alllower: String, someName: String, someLongName: String)

  final case class Data(id: Long, udt: Udt)

  private val data =
    Data(0, Udt("ALL-UPPER", "all-lower", "some-name", "some-long-name"))

  override def spec: Spec[Session, Any] = suite("UdtWrites")(
    test("should write names as is") {
      implicit val configuration: Configuration = Configuration(identity(_))

      val query =
        cql"insert into ${lift(keyspace)}.udt_codec_default_name_test(id, udt) values (${data.id}, ${data.udt})".execute

      query.as(assertCompletes)
    },
    test("should write names transformed to snake_case") {
      val query =
        cql"insert into ${lift(keyspace)}.udt_codec_snake_name_test(id, udt) values (${data.id}, ${data.udt})".execute

      query.as(assertCompletes)
    }
  )

}
