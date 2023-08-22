package com.ringcentral.cassandra4io

import cats.Functor
import cats.syntax.functor._
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder
import com.ringcentral.cassandra4io.query.Query
import com.ringcentral.cassandra4io.CassandraSession
import com.datastax.oss.driver.api.core.cql.BatchType
import com.ringcentral.cassandra4io.query

class Batch[F[_]: Functor](batchStatementBuilder: BatchStatementBuilder) {
  def add(queries: Seq[Query[F, _]])                                           = new Batch[F](batchStatementBuilder.addStatements(queries.map(_.statement): _*))
  def execute(session: CassandraSession[F]): F[Boolean]                        =
    session.execute(batchStatementBuilder.build()).map(_.wasApplied)
  def config(config: BatchStatementBuilder => BatchStatementBuilder): Batch[F] =
    new Batch[F](config(batchStatementBuilder))
}

object Batch {
  def logged[F[_]: Functor]   = new Batch[F](new BatchStatementBuilder(BatchType.LOGGED))
  def unlogged[F[_]: Functor] = new Batch[F](new BatchStatementBuilder(BatchType.UNLOGGED))
}
