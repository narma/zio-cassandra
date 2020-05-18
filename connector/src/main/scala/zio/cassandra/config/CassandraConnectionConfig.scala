package zio.cassandra.config

import java.net.InetSocketAddress
import zio.config._
import ConfigDescriptor._
import zio.config.ConfigDocs.{Leaf => _}
import zio.config.typesafe.TypesafeConfigSource

import scala.util.Try

case class CassandraConnectionConfig(contactPoints: Seq[InetSocketAddress],
                                     username: Option[String] = None,
                                     password: Option[String] = None,
                                     defaultProfileName: Option[String])

object CassandraConnectionConfig {

  val DEFAULT_PORT = 9042

  def applyRaw(rawContactPoints: Seq[String],
               username: Option[String],
               password: Option[String],
               profileName: Option[String]): CassandraConnectionConfig = {
    val contactPoints = rawContactPoints.flatMap {
      _.split(":") match {
        case s if s.size == 2 => Try(new InetSocketAddress(s.head, Try(s(1).toInt).getOrElse(DEFAULT_PORT))).toOption
        case s if s.size == 1 => Try(new InetSocketAddress(s.head, DEFAULT_PORT)).toOption
        case _                => Option.empty[InetSocketAddress]
      }
    }

    CassandraConnectionConfig(contactPoints, username, password, profileName)
  }

  def unapply(arg: CassandraConnectionConfig): Option[(List[String], Option[String], Option[String], Option[String])] =
    Some((arg.contactPoints.map(_.toString).toList, arg.username, arg.password, arg.defaultProfileName))

  val config: _root_.zio.config.ConfigDescriptor[CassandraConnectionConfig] = (
    list("contact-points")(string)
      |@| string("username").optional
      |@| string("password").optional
      |@| string("profile-name").optional
  )(CassandraConnectionConfig.applyRaw, CassandraConnectionConfig.unapply)

  val cassandraConfig: _root_.zio.config.ConfigDescriptor[CassandraConnectionConfig] = nested("cassandra") { config }

  def apply(config: com.typesafe.config.Config): Either[Throwable, CassandraConnectionConfig] =
    TypesafeConfigSource
      .fromTypesafeConfig(config)
      .swap
      .map(new RuntimeException(_))
      .swap
      .flatMap(x => read(cassandraConfig.from(x)))
}
