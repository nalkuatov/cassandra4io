package com.ringcentral.cassandra4io

import cats.Functor
import cats.data.OptionT
import cats.syntax.functor.*
import cats.syntax.flatMap.*
import fs2.Stream
import com.ringcentral.cassandra4io.cql.*
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement}
import cats.Monad
import com.ringcentral.cassandra4io.codec.Reads

object query {
  case class QueryTemplate[V <: Tuple: Binder, R: Reads] private[cassandra4io] (
    query: String,
    config: BoundStatement => BoundStatement
  ) {
    def +(that: String): QueryTemplate[V, R] = QueryTemplate[V, R](this.query + that, config)

    def ++[W <: Tuple](that: QueryTemplate[W, R])(implicit
      binderForW: Binder[W],
      binderForOut: Binder[Tuple.Concat[V, W]]
    ): QueryTemplate[Tuple.Concat[V, W], R] = concat(that)

    def concat[W <: Tuple](that: QueryTemplate[W, R])(implicit
      binderForW: Binder[W],
      binderForOut: Binder[Tuple.Concat[V, W]]
    ): QueryTemplate[Tuple.Concat[V, W], R] = QueryTemplate[Tuple.Concat[V, W], R](
      this.query + that.query,
      this.config andThen that.config
    )

    def as[R1: Reads]: QueryTemplate[V, R1] = QueryTemplate[V, R1](query, config)

    def prepare[F[_]: Functor](session: CassandraSession[F]): F[PreparedQuery[F, V, R]] =
      session.prepare(query).map(new PreparedQuery(session, _, config))

    def config(config: BoundStatement => BoundStatement): QueryTemplate[V, R] =
      QueryTemplate[V, R](this.query, this.config andThen config)

    def stripMargin: QueryTemplate[V, R] = QueryTemplate[V, R](this.query.stripMargin, this.config)
  }

  case class ParameterizedQuery[V <: Tuple: Binder, R: Reads] private[cassandra4io] (template: QueryTemplate[V, R], values: V) {

    override def toString(): String = s"(query: ${template.query}, values: $values)"

    def +(that: String): ParameterizedQuery[V, R] = ParameterizedQuery[V, R](this.template + that, this.values)

    def ++[W <: Tuple](that: ParameterizedQuery[W, R])(implicit
      binderForW: Binder[W],
      binderForOut: Binder[Tuple.Concat[V, W]]
    ): ParameterizedQuery[Tuple.Concat[V, W], R] = concat(that)

    def concat[W <: Tuple](that: ParameterizedQuery[W, R])(implicit
      binderForW: Binder[W],
      binderForOut: Binder[Tuple.Concat[V, W]]
    ): ParameterizedQuery[Tuple.Concat[V, W], R] =
      ParameterizedQuery[Tuple.Concat[V, W], R](this.template ++ that.template, this.values ++ that.values)

    def as[R1: Reads]: ParameterizedQuery[V, R1] = ParameterizedQuery[V, R1](template.as[R1], values)

    def select[F[_]: Functor](session: CassandraSession[F]): Stream[F, R] =
      Stream.force(template.prepare(session).map(_.apply(values).select))

    def selectFirst[F[_]: Monad](session: CassandraSession[F]): F[Option[R]] =
      template.prepare(session).flatMap(_.apply(values).selectFirst)

    def execute[F[_]: Monad](session: CassandraSession[F]): F[Boolean] =
      template.prepare(session).map(_.apply(values)).flatMap(_.execute)

    def config(config: BoundStatement => BoundStatement): ParameterizedQuery[V, R] =
      ParameterizedQuery[V, R](template.config(config), values)

    def stripMargin: ParameterizedQuery[V, R] = ParameterizedQuery[V, R](this.template.stripMargin, values)
  }

  class PreparedQuery[F[_]: Functor, V <: Tuple: Binder, R: Reads] private[query] (
    session: CassandraSession[F],
    statement: PreparedStatement,
    config: BoundStatement => BoundStatement
  ) {
    def apply(values: V) = new Query[F, R](session, Binder[V].bind(config(statement.bind()), 0, values)._1)
  }

  class Query[F[_]: Functor, R: Reads] private[query] (
    session: CassandraSession[F],
    private[cassandra4io] val statement: BoundStatement
  ) {
    override def toString(): String = s"$statement"
    def config(statement: BoundStatement => BoundStatement) = new Query[F, R](session, statement(this.statement))
    def select: Stream[F, R]                                = session.select(statement).map(Reads[R].read(_))
    def selectFirst: F[Option[R]]                           = OptionT(session.selectFirst(statement)).map(Reads[R].read(_)).value
    def execute: F[Boolean]                                 = session.execute(statement).map(_.wasApplied)
  }
}
