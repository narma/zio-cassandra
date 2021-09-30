package zio

package object cassandra {
  type Session = Has[service.CassandraSession]
}
