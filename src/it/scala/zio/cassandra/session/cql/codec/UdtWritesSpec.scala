package zio.cassandra.session.cql.codec

import zio.cassandra.session.CassandraSpecUtils
import zio.cassandra.session.cql.CqlStringContext
import zio.cassandra.session.cql.unsafe.lift
import zio.test._

object UdtWritesSpec extends CassandraSpecUtils {

  final case class Udt(ALLUPPER: String, alllower: String, someName: String, someLongName: String)

  final case class Data(id: Long, udt: Udt)

  private val data =
    Data(0, Udt("ALL-UPPER", "all-lower", "some-name", "some-long-name"))

  val writesTests = suite("UdtWrites")(
    testM("should write names as is") {
      implicit val configuration: Configuration = Configuration(identity(_))

      val query =
        cql"insert into ${lift(keyspace)}.udt_codec_default_name_test(id, udt) values (${data.id}, ${data.udt})".execute

      query.as(assertCompletes)
    },
    testM("should write names transformed to snake_case") {
      val query =
        cql"insert into ${lift(keyspace)}.udt_codec_snake_name_test(id, udt) values (${data.id}, ${data.udt})".execute

      query.as(assertCompletes)
    }
  )

}
