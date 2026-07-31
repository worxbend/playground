/*
 * Copyright (c) 2020 Worxbend
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.worxbend.persistence

import com.augustnagro.magnum.Frag
import java.lang.reflect.InvocationHandler
import java.lang.reflect.Proxy
import java.sql.Connection
import java.sql.PreparedStatement
import scala.collection.mutable

/** A `PreparedStatement` that records what was bound to it, and nothing else.
  *
  * The whole security argument of this module is "user input reaches the database only as a bind parameter", and the
  * only way to check that claim end to end without a database is to watch the JDBC calls. A dynamic proxy is used
  * rather than a hand-written stub because `PreparedStatement` has well over a hundred methods and a stub would have to
  * be edited every time a codec starts using a different setter — exactly the case a test must not miss.
  *
  * The proxy answers `getConnection` with a second proxy so that array codecs, which go through
  * `Connection.createArrayOf`, work and have their elements recorded too.
  */
object Recording:

  /** What a fragment did to a statement. */
  final case class Bindings(positions: Vector[Int], values: Vector[Any], arrayElements: Vector[Any]):

    /** Every value that crossed the JDBC boundary, arrays flattened. */
    def all: Vector[Any] = values ++ arrayElements

  final private class Recorder:
    val positions: mutable.ArrayBuffer[Int] = mutable.ArrayBuffer.empty
    val values: mutable.ArrayBuffer[Any] = mutable.ArrayBuffer.empty
    val arrayItems: mutable.ArrayBuffer[Any] = mutable.ArrayBuffer.empty

  /** Runs a fragment's writer against a recording statement, starting at JDBC position 1.
    *
    * @return
    *   the next free position the writer reported, and everything it bound.
    */
  def bind(frag: Frag): (Int, Bindings) =
    val recorder = Recorder()
    val statement = statementProxy(recorder)
    val next = frag.writer.write(statement, 1)
    (next, Bindings(recorder.positions.toVector, recorder.values.toVector, recorder.arrayItems.toVector))

  private def statementProxy(recorder: Recorder): PreparedStatement =
    val connection = connectionProxy(recorder)
    val handler: InvocationHandler = (_, method, args) =>
      val arguments = Option(args).getOrElse(Array.empty[Object])
      method.getName match
        case "getConnection"                                           => connection
        case "toString"                                                => "recording-statement"
        case "hashCode"                                                => Integer.valueOf(1)
        case "equals"                                                  => java.lang.Boolean.FALSE
        case name if name.startsWith("set") && isPositional(arguments) =>
          recorder.positions += arguments(0).asInstanceOf[Integer].intValue
          recorder.values += (if arguments.length > 1 then arguments(1) else null)
          null
        case _ => zeroOf(method.getReturnType)
    Proxy
      .newProxyInstance(getClass.getClassLoader, Array(classOf[PreparedStatement]), handler)
      .asInstanceOf[PreparedStatement]

  private def connectionProxy(recorder: Recorder): Connection =
    val handler: InvocationHandler = (_, method, args) =>
      val arguments = Option(args).getOrElse(Array.empty[Object])
      method.getName match
        case "createArrayOf" =>
          // (typeName, elements) — the elements are the interesting half: they are the user's values.
          arguments.lift(1).foreach {
            case elements: Array[?] => recorder.arrayItems ++= elements.toVector
            case other              => recorder.arrayItems += other
          }
          null
        case "toString" => "recording-connection"
        case "hashCode" => Integer.valueOf(2)
        case "equals"   => java.lang.Boolean.FALSE
        case _          => zeroOf(method.getReturnType)
    Proxy.newProxyInstance(getClass.getClassLoader, Array(classOf[Connection]), handler).asInstanceOf[Connection]

  /** A `ResultSet` that answers `getString` from a fixed column map and nothing else. Enough to exercise a read codec
    * without a database, and small enough that it cannot quietly start answering something it should not.
    */
  def resultSet(columns: Map[Int, String]): java.sql.ResultSet =
    val handler: InvocationHandler = (_, method, args) =>
      val arguments = Option(args).getOrElse(Array.empty[Object])
      method.getName match
        case "getString" if isPositional(arguments) => columns.getOrElse(arguments(0).asInstanceOf[Integer], null)
        case "toString"                             => "recording-result-set"
        case "hashCode"                             => Integer.valueOf(3)
        case "equals"                               => java.lang.Boolean.FALSE
        case _                                      => zeroOf(method.getReturnType)
    Proxy
      .newProxyInstance(getClass.getClassLoader, Array(classOf[java.sql.ResultSet]), handler)
      .asInstanceOf[java.sql.ResultSet]

  private def isPositional(arguments: Array[Object]): Boolean =
    arguments.nonEmpty && arguments(0).isInstanceOf[Integer]

  /** A proxy must never return `null` where the interface declares a primitive — the unboxing happens in generated code
    * and the NPE would be attributed to the codec under test rather than to this helper.
    */
  private def zeroOf(returnType: Class[?]): Object =
    if returnType == java.lang.Boolean.TYPE then java.lang.Boolean.FALSE
    else if returnType == java.lang.Integer.TYPE then Integer.valueOf(0)
    else if returnType == java.lang.Long.TYPE then java.lang.Long.valueOf(0L)
    else if returnType == java.lang.Double.TYPE then java.lang.Double.valueOf(0.0)
    else if returnType == java.lang.Float.TYPE then java.lang.Float.valueOf(0.0f)
    else if returnType == java.lang.Short.TYPE then java.lang.Short.valueOf(0.toShort)
    else if returnType == java.lang.Byte.TYPE then java.lang.Byte.valueOf(0.toByte)
    else if returnType == java.lang.Character.TYPE then java.lang.Character.valueOf(0.toChar)
    else null

/** Static analysis of a compiled fragment's text. */
object SqlText:

  /** Counts JDBC placeholders, honouring pgjdbc's `??` escape.
    *
    * A naive `count('?')` would be wrong in exactly the place it matters: `data @?? ?::jsonpath` contains three `?`
    * characters and one placeholder. Getting this right is what makes the parameter-count property meaningful rather
    * than a test that passes because both sides count the same wrong thing.
    */
  def placeholders(sql: String): Int =
    @annotation.tailrec
    def loop(index: Int, count: Int): Int =
      if index >= sql.length then count
      else if sql.charAt(index) != '?' then loop(index + 1, count)
      else if index + 1 < sql.length && sql.charAt(index + 1) == '?' then loop(index + 2, count)
      else loop(index + 1, count + 1)
    loop(0, 0)

  /** True when parentheses nest correctly and close. A fragment that balances by luck rather than by construction is a
    * fragment that will stop balancing the first time someone nests an `Or` inside a `Not`.
    */
  def balanced(sql: String): Boolean =
    @annotation.tailrec
    def loop(index: Int, depth: Int): Boolean =
      if depth < 0 then false
      else if index >= sql.length then depth == 0
      else
        sql.charAt(index) match
          case '(' => loop(index + 1, depth + 1)
          case ')' => loop(index + 1, depth - 1)
          case _   => loop(index + 1, depth)
    loop(0, 0)
