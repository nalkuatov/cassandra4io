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
import scala.language.implicitConversions
import scala.annotation.{ implicitNotFound, tailrec }
import scala.jdk.CollectionConverters._
import com.ringcentral.cassandra4io.codec.CellWrites

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
    implicit def emptyTupleBinder: Binder[EmptyTuple] = (st, index, _) => (st, index)

    /** This typeclass instance is used to (inductively) derive datatypes that can have arbitrary amounts of nesting
    * @param writes
    *   is evidence that a typeclass instance of CellWrites exists for A
    * @tparam T
    *   is the Scala datatype that needs to be written to Cassandra
    * @return
    */
    implicit def deriveBinderFromCellWrites[T](implicit writes: CellWrites[T]): Binder[T] = new Binder[T] {
      override def bind(statement: BoundStatement, index: Int, value: T): (BoundStatement, Int) = {
        val protocol = statement.protocolVersion()
        val dataType = statement.getType(index)
        val bytes    = writes.write(value, protocol, dataType)
        (statement.setBytesUnsafe(index, bytes), index + 1)
      }
    }
  }
}
