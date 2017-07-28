package com.avvo.data.flume.extensions

import java.io._
import java.nio.ByteBuffer

import com.typesafe.scalalogging.LazyLogging
import net.fosdal.oslo._
import net.fosdal.oslo.oany._
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileStream, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.EncoderFactory
import org.apache.flume.conf.Configurable
import org.apache.flume.serialization.AvroEventSerializerConfigurationConstants._
import org.apache.flume.serialization.EventSerializer
import org.apache.flume.{Context, Event, FlumeException}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Serialize multiple Avro files using given schema based on the rollup size.
  *
  * To avoid overhead of too many small size files on HDFS, AvroEventRollupSerializer helps to
  * serialize multiple Avro files with same schema together into a new larger size file. It doesn't
  * change schema and data block of original Avro file. Serialized Avro file consists with Avro
  * schema and data blocks extracted from raw Avro files. The Avro schema is determined by reading
  * a Flume event header. It should be passed from HDFS.
  *
  * Flume config setup example
  *
  * flumeAgentName.sinks.sink1.type = hdfs
  * flumeAgentName.sinks.sink1.serializer = com.avvo.data.flume.extensions.AvroEventRollupSerializer$Builder
  * flumeAgentName.sinks.sink1.serializer.avroSchemaPath = hdfs:///path/schemaFileName.avsc
  *
  * flume-ng agent -n flumeAgentName -f ./flume.config -C ./flume-extensions-assembly-0.1.jar
  */
object AvroEventRollupSerializer {

  val AvroSchemaPath = "avroSchemaPath"

  class Builder extends EventSerializer.Builder {
    override def build(context: Context, out: OutputStream): EventSerializer = {
      new AvroEventRollupSerializer(out) tap { (ser: AvroEventRollupSerializer) =>
        ser.configure(context)
      }
    }
  }

}

class AvroEventRollupSerializer(out: OutputStream) extends EventSerializer with Configurable with LazyLogging {

  var context: Option[Context] = None
  var schema: Option[Schema] = None
  var count = 1
  lazy val compressionCodec  = context.get.getString(COMPRESSION_CODEC, DEFAULT_COMPRESSION_CODEC)
  lazy val syncIntervalBytes = context.get.getInteger(SYNC_INTERVAL_BYTES, DEFAULT_SYNC_INTERVAL_BYTES)
  lazy val dataFileWriter    = initialize()

  val reader = new GenericDatumReader[AnyRef]

  override def configure(context: Context): Unit = this.context = Some(context)

  @throws[IOException]
  override def afterCreate(): Unit = ()

  @throws[IOException]
  override def afterReopen(): Unit = {
    // impossible to initialize DataFileWriter without writing the schema?
    throw new UnsupportedOperationException("Avro API doesn't support append")
  }

  @throws[IOException]
  def serialize(value: AnyRef): Array[Byte] = {
    using(new ByteArrayOutputStream()) { outputStream =>
      val encoder = EncoderFactory.get.directBinaryEncoder(outputStream, null) // scalastyle:ignore
      val writer  = new GenericDatumWriter[GenericData.Record](value.asInstanceOf[GenericData.Record].getSchema)
      writer.write(value.asInstanceOf[GenericData.Record], encoder)
      outputStream.toByteArray
    }
  }

  def readSchemaFromEvent(flumeEventBody: Array[Byte]): Schema = {
    val inStream = new ByteArrayInputStream(flumeEventBody)
    using(new DataFileStream[AnyRef](inStream, reader)) { fileReader =>
      fileReader.getSchema
    }
  }

  def readAvroData(flumeEventBody: Array[Byte]): Array[Byte] = {
    val inStream = new ByteArrayInputStream(flumeEventBody)
    using(new DataFileStream[AnyRef](inStream, reader)) { fileReader =>
      serialize(fileReader.next)
    }
  }

  @throws[IOException]
  private def initialize(): DataFileWriter[AnyRef] = {
    logger.info(s"schema is defined: ${schema.isDefined}")
    val writer         = new GenericDatumWriter[AnyRef](schema.get)
    val dataFileWriter = new DataFileWriter(writer)
    dataFileWriter.setSyncInterval(syncIntervalBytes)

    try {
      val codecFactory = CodecFactory.fromString(compressionCodec)
      dataFileWriter.setCodec(codecFactory)
    } catch {
      case e: Throwable =>
        logger.warn(s"""Unable to instantiate avro codec with name, "$compressionCodec", compression disabled""", e)
    }
    dataFileWriter.create(schema.get, out)
  }

  @throws[IOException]
  override def write(event: Event): Unit = {
    logger.info(s"writing event #${count += 1}")
    if (!schema.isDefined) {
      logger.info("reading schema from event for first event")
      schema = Some(readSchemaFromEvent(event.getBody))
    }
    dataFileWriter.appendEncoded(ByteBuffer.wrap(readAvroData(event.getBody)))
  }

  @throws[IOException]
  override def flush(): Unit = dataFileWriter.flush()

  @throws[IOException]
  override def beforeClose(): Unit = ()

  override def supportsReopen: Boolean = false

}
