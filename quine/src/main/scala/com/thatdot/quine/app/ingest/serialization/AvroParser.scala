package com.thatdot.quine.app.ingest.serialization

import com.thatdot.quine.graph.cypher
// import com.thatdot.quine.graph.cypher.Expr

import cats.Monad
import cats.syntax.all._
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import scala.language.higherKinds
import scala.util.Try

class AvroParser[F[_]: Monad](deserializer: KafkaAvroDeserializer) {

  private def bytesToGenericRecord(bytes: Array[Byte]): F[GenericRecord] = ???

  private def genericRecordToCypherValue(record: GenericRecord): F[cypher.Value] = ???

  def parseBytes(bytes: Array[Byte]): F[cypher.Value] = 
    for {
      record <- bytesToGenericRecord(bytes)
      value <- genericRecordToCypherValue(record)
    } yield value
}

object AvroParser {
  //TODO
  def apply(): AvroParser[Try] = 
    new AvroParser[Try](null)
}
