package zio.cassandra.session.cql

import com.datastax.oss.driver.api.core.cql.Row
import shapeless.{ ::, HList, HNil, ProductArgs }
import zio.cassandra.session.cql.query.{ ParameterizedQuery, QueryTemplate }

import scala.annotation.nowarn

class CqlTemplateStringInterpolator(ctx: StringContext) extends ProductArgs {
  import CqlTemplateStringInterpolator._
  @nowarn("msg=is never used")
  def applyProduct[P <: HList, V <: HList](params: P)(implicit
    bb: BindableBuilder.Aux[P, V]
  ): QueryTemplate[V, Row] = {
    implicit val binder: Binder[V] = bb.binder
    QueryTemplate[V, Row](ctx.parts.mkString("?"), identity)
  }
}

object CqlTemplateStringInterpolator {

  trait BindableBuilder[P] {
    type Repr <: HList
    def binder: Binder[Repr]
  }

  object BindableBuilder {
    type Aux[P, Repr0] = BindableBuilder[P] { type Repr = Repr0 }
    def apply[P](implicit builder: BindableBuilder[P]): BindableBuilder.Aux[P, builder.Repr] = builder
    implicit def hNilBindableBuilder: BindableBuilder.Aux[HNil, HNil]                        = new BindableBuilder[HNil] {
      override type Repr = HNil
      override def binder: Binder[HNil] = Binder[HNil]
    }
    implicit def hConsBindableBuilder[PH <: Put[_], T: Binder, PT <: HList, RT <: HList](implicit
      f: BindableBuilder.Aux[PT, RT]
    ): BindableBuilder.Aux[Put[T] :: PT, T :: RT] = new BindableBuilder[Put[T] :: PT] {
      override type Repr = T :: RT
      override def binder: Binder[T :: RT] = {
        implicit val tBinder: Binder[RT] = f.binder
        Binder[T :: RT]
      }
    }
  }
}

class CqlStringInterpolator(ctx: StringContext) extends ProductArgs {
  def applyProduct[V <: HList: Binder](values: V): ParameterizedQuery[V, Row] =
    ParameterizedQuery[V, Row](QueryTemplate[V, Row](ctx.parts.mkString("?"), identity), values)
}
