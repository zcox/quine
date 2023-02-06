package com.thatdot.quine.app.ingest.serialization

import com.thatdot.quine.graph.cypher

import cats._
import cats.syntax.all._
import io.confluent.kafka.serializers._
import org.apache.avro.generic._
import org.apache.avro.io.EncoderFactory
import scala.language.higherKinds
import scala.util.Try
import io.circe.parser._
import java.net.URL
import scala.jdk.CollectionConverters._
import java.io.ByteArrayOutputStream

class AvroParser[F[_]: MonadThrow](deserializer: KafkaAvroDeserializer) {

  private def wrap[A](a: => A): F[A] = 
    MonadThrow[F].catchNonFatal(a)

  private def bytesToGenericRecord(bytes: Array[Byte]): F[GenericRecord] = 
    //note that the topic name is unnecessary, since the avro bytes contain the ID of the writer schema in the registry
    wrap(deserializer.deserialize(null, bytes))
      .flatMap {
        case r: GenericRecord => 
          println(s"***** record = $r")
          r.pure[F]
        //TODO better error
        case o => MonadThrow[F].raiseError(new RuntimeException(s"Did not deserialize to GenericRecord: $o"))
      }

  //TODO return bytes and use CirceSupportParser like CypherJsonInputFormat
  private def recordToJsonString(r: GenericRecord): String = {
    val schema = r.getSchema
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val baos = new ByteArrayOutputStream
    val jsonEncoder = EncoderFactory.get.jsonEncoder(schema, baos)
    writer.write(r, jsonEncoder)
    jsonEncoder.flush
    baos.toString()
  }

  private def genericRecordToCypherValue(record: GenericRecord): F[cypher.Value] = 
    for {
      s <- wrap(recordToJsonString(record))
      () = println(s"***** string = $s")
      j <- parse(s).liftTo[F]
      () = println(s"***** json = $j")
      v <- wrap(cypher.Value.fromCirceJson(j))
      () = println(s"***** cypher value = $v")
    } yield v

  def parseBytes(bytes: Array[Byte]): F[cypher.Value] = 
    for {
      record <- bytesToGenericRecord(bytes)
      value <- genericRecordToCypherValue(record)
    } yield value
}

object AvroParser {

  def apply(schemaRegistryUrl: URL): AvroParser[Try] = {
    val d = new KafkaAvroDeserializer()
    d.configure(Map(
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryUrl.toString(),
    ).asJava, false)
    new AvroParser[Try](d)
  }
}
