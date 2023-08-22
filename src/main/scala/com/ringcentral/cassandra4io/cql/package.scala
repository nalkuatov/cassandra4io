package com.ringcentral.cassandra4io

import cats.data.OptionT
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.{ Functor, Monad }
import com.datastax.oss.driver.api.core.`type`.UserDefinedType
import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.api.core.data.UdtValue
import com.datastax.oss.driver.internal.core.`type`.{ DefaultListType, DefaultMapType, DefaultSetType }
import fs2.Stream

import java.nio.ByteBuffer
import java.time.{ Instant, LocalDate }
import java.util.UUID
import scala.annotation.{ implicitNotFound, tailrec }
import scala.jdk.CollectionConverters._

package object cql {

  /**
   * BoundValue is used to capture the value inside the cql interpolated string along with evidence of its Binder so that
   * a ParameterizedQuery can be built and the values can be bound to the BoundStatement internally
   */
  final case class BoundValue[A](value: A, ev: Binder[A])
  object BoundValue {
    // This implicit conversion automatically captures the value and evidence of the Binder in a cql interpolated string
    implicit def aToBoundValue[A](a: A)(implicit ev: Binder[A]): BoundValue[A] =
      BoundValue(a, ev)
  }

  implicit class UnsetOptionValueOps[A](val self: Option[A]) extends AnyVal {
    def usingUnset(implicit aBinder: Binder[A]): BoundValue[Option[A]] =
      BoundValue(self, Binder.optionUsingUnsetBinder[A])
  }

  @implicitNotFound("""Cannot find or construct a Binder instance for type:

  ${T}

  Construct it if needed, please refer to Binder source code for guidance
""")
  trait Binder[T] { self =>
    def bind(statement: BoundStatement, index: Int, value: T): (BoundStatement, Int)

    def contramap[U](f: U => T): Binder[U] = new Binder[U] {
      override def bind(statement: BoundStatement, index: Int, value: U): (BoundStatement, Int) =
        self.bind(statement, index, f(value))
    }
  }

  trait Put[T]
  object Put {
    def apply[T: Binder]: Put[T] = new Put[T] {}
  }

  object Binder extends BinderLowerPriority with BinderLowestPriority {

    def apply[T](implicit binder: Binder[T]): Binder[T] = binder

    implicit val stringBinder: Binder[String] = new Binder[String] {
      override def bind(statement: BoundStatement, index: Int, value: String): (BoundStatement, Int) =
        (statement.setString(index, value), index + 1)
    }

    implicit val doubleBinder: Binder[Double] = new Binder[Double] {
      override def bind(statement: BoundStatement, index: Int, value: Double): (BoundStatement, Int) =
        (statement.setDouble(index, value), index + 1)
    }

    implicit val intBinder: Binder[Int] = new Binder[Int] {
      override def bind(statement: BoundStatement, index: Int, value: Int): (BoundStatement, Int) =
        (statement.setInt(index, value), index + 1)
    }

    implicit val longBinder: Binder[Long] = new Binder[Long] {
      override def bind(statement: BoundStatement, index: Int, value: Long): (BoundStatement, Int) =
        (statement.setLong(index, value), index + 1)
    }

    implicit val byteBufferBinder: Binder[ByteBuffer] = new Binder[ByteBuffer] {
      override def bind(statement: BoundStatement, index: Int, value: ByteBuffer): (BoundStatement, Int) =
        (statement.setByteBuffer(index, value), index + 1)
    }

    implicit val localDateBinder: Binder[LocalDate] = new Binder[LocalDate] {
      override def bind(statement: BoundStatement, index: Int, value: LocalDate): (BoundStatement, Int) =
        (statement.setLocalDate(index, value), index + 1)
    }

    implicit val instantBinder: Binder[Instant] = new Binder[Instant] {
      override def bind(statement: BoundStatement, index: Int, value: Instant): (BoundStatement, Int) =
        (statement.setInstant(index, value), index + 1)
    }

    implicit val booleanBinder: Binder[Boolean] = new Binder[Boolean] {
      override def bind(statement: BoundStatement, index: Int, value: Boolean): (BoundStatement, Int) =
        (statement.setBoolean(index, value), index + 1)
    }

    implicit val uuidBinder: Binder[UUID] = new Binder[UUID] {
      override def bind(statement: BoundStatement, index: Int, value: UUID): (BoundStatement, Int) =
        (statement.setUuid(index, value), index + 1)
    }

    implicit val bigIntBinder: Binder[BigInt] = new Binder[BigInt] {
      override def bind(statement: BoundStatement, index: Int, value: BigInt): (BoundStatement, Int) =
        (statement.setBigInteger(index, value.bigInteger), index + 1)
    }

    implicit val bigDecimalBinder: Binder[BigDecimal] = new Binder[BigDecimal] {
      override def bind(statement: BoundStatement, index: Int, value: BigDecimal): (BoundStatement, Int) =
        (statement.setBigDecimal(index, value.bigDecimal), index + 1)
    }

    implicit val shortBinder: Binder[Short] = new Binder[Short] {
      override def bind(statement: BoundStatement, index: Int, value: Short): (BoundStatement, Int) =
        (statement.setShort(index, value), index + 1)
    }

    implicit val userDefinedTypeValueBinder: Binder[UdtValue] =
      (statement: BoundStatement, index: Int, value: UdtValue) => (statement.setUdtValue(index, value), index + 1)

    private def commonOptionBinder[T: Binder](
      bindNone: (BoundStatement, Int) => BoundStatement
    ): Binder[Option[T]] = new Binder[Option[T]] {
      override def bind(statement: BoundStatement, index: Int, value: Option[T]): (BoundStatement, Int) = value match {
        case Some(x) => Binder[T].bind(statement, index, x)
        case None => (bindNone(statement, index), index + 1)
      }
    }

    implicit def optionBinder[T: Binder]: Binder[Option[T]] = commonOptionBinder[T] { (statement, index) =>
      statement.setToNull(index)
    }

    def optionUsingUnsetBinder[T: Binder]: Binder[Option[T]] = commonOptionBinder[T] { (statement, index) =>
      statement.unset(index)
    }

    implicit class UdtValueBinderOps(udtBinder: Binder[UdtValue]) {

      /**
       * This is necessary for UDT values as you are not allowed to safely create a UDT value, instead you use the
       * prepared statement's variable definitions to retrieve a UserDefinedType that can be used as a constructor
       * for a UdtValue
       *
       * @param f is a function that accepts the input value A along with a constructor that you use to build the
       *          UdtValue that gets sent to Cassandra
       * @tparam A
       * @return
       */
      def contramapUDT[A](f: (A, UserDefinedType) => UdtValue): Binder[A] = new Binder[A] {
        override def bind(statement: BoundStatement, index: Int, value: A): (BoundStatement, Int) = {
          val udtValue = f(
            value,
            statement.getPreparedStatement.getVariableDefinitions.get(index).getType.asInstanceOf[UserDefinedType]
          )
          udtBinder.bind(statement, index, udtValue)
        }
      }
    }
  }

  trait BinderLowerPriority {

    /**
     * This typeclass instance is used to (inductively) derive datatypes that can have arbitrary amounts of nesting
     * @param ev is evidence that a typeclass instance of CassandraTypeMapper exists for A
     * @tparam A is the Scala datatype that needs to be written to Cassandra
     * @return
     */
    implicit def deriveBinderFromCassandraTypeMapper[A](implicit ev: CassandraTypeMapper[A]): Binder[A] =
      (statement: BoundStatement, index: Int, value: A) => {
        val datatype  = statement.getType(index)
        val cassandra = ev.toCassandra(value, datatype)
        (statement.set(index, cassandra, ev.classType), index + 1)
      }
  }
}
