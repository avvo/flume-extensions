package com.avvo.data.flume.extensions

/**
  * What's this file for
  */

import java.io._
import java.net.URL
import java.nio.ByteBuffer

import net.fosdal.oslo._
import org.apache.avro.file.{CodecFactory, DataFileStream, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{DatumWriter, EncoderFactory}
import org.apache.avro.{AvroRuntimeException, Schema}
import org.apache.flume.conf.Configurable
import org.apache.flume.serialization.AvroEventSerializerConfigurationConstants._
import org.apache.flume.serialization.EventSerializer
import org.apache.flume.{Context, Event, FlumeException}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory


object AvroEventRollupSerializer {
  private val logger = LoggerFactory.getLogger(classOf[AvroEventRollupSerializer])

  class Builder extends EventSerializer.Builder {
    def build(context: Context, out: OutputStream): EventSerializer = {
      val writer = new AvroEventRollupSerializer(out)
      writer.configure(context)
      writer
    }
  }
}

class AvroEventRollupSerializer(out: OutputStream) extends EventSerializer with Configurable {
  var writer: Option[DatumWriter[AnyRef]] = None
  var reader = new GenericDatumReader[AnyRef]
  var dataFileWriter: Option[DataFileWriter[AnyRef]] = None
  var syncIntervalBytes = 0
  var compressionCodec: Option[String] = None
  var schema: Option[Schema] = None
  var schemaPath: Option[String] = None
  val AVRO_SCHEMA_PATH = "avroSchemaPath"

  def configure(context: Context): Unit = {
    syncIntervalBytes = context.getInteger(SYNC_INTERVAL_BYTES, DEFAULT_SYNC_INTERVAL_BYTES)
    compressionCodec = Some(context.getString(COMPRESSION_CODEC, DEFAULT_COMPRESSION_CODEC))
    schemaPath = Some(context.getString(AVRO_SCHEMA_PATH))
  }

  @throws[IOException]
  def loadSchema(schemaFilePath: String): Option[Schema] = {
    val parser = new Schema.Parser()
    if (schemaFilePath.toLowerCase.startsWith("hdfs://")) {
      val fs = FileSystem.get(new Configuration())
      using(fs.open(new Path(schemaFilePath))) { input =>
          Some(parser.parse(input))
      }
    }
    else {
      using(new URL(schemaFilePath).openStream()) { inputStream =>
        Some(parser.parse(inputStream))
      }
    }
  }

  @throws[IOException]
  def afterCreate() = {
    // no-op
  }

  @throws[IOException]
  def afterReopen(): Unit = { // impossible to initialize DataFileWriter without writing the schema?
    throw new UnsupportedOperationException("Avro API doesn't support append")
  }

  @throws[IOException]
  def serialize(value: AnyRef): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.directBinaryEncoder(outputStream, null)
    val writer = new GenericDatumWriter[GenericData.Record](value.asInstanceOf[GenericData.Record].getSchema)
    writer.write(value.asInstanceOf[GenericData.Record], encoder)
    outputStream.toByteArray
  }

  def readAvroData(flumeEventBody: Array[Byte]): Array[Byte] = {
    val inStream = new ByteArrayInputStream(flumeEventBody)
    val fileReader = new DataFileStream[AnyRef](inStream, reader)

    serialize(fileReader.next)
  }

  @throws[IOException]
  def write(event: Event): Unit = {
    if (dataFileWriter.isEmpty) {
      initialize(event)
    }
    AvroEventRollupSerializer.logger.debug(if (schema.isDefined) "write: schema is defined" else "write(): 2" )
    dataFileWriter.get.appendEncoded(ByteBuffer.wrap(readAvroData(event.getBody)))
  }

  @throws[IOException]
  private def initialize(event: Event) = {
    if (schemaPath.isEmpty) {
      throw new FlumeException("SchemaPath is not initialized")
    }
    schema = loadSchema(schemaPath.get)

    AvroEventRollupSerializer.logger.debug(if (schema.isDefined) "schema is defined" else "2" )
    writer = Some(new GenericDatumWriter[AnyRef](schema.get))

    dataFileWriter = Some(new DataFileWriter(writer.get))
    dataFileWriter.get.setSyncInterval(syncIntervalBytes)
    try {
      val codecFactory = CodecFactory.fromString(compressionCodec.get)
      dataFileWriter.get.setCodec(codecFactory)
    } catch {
      case e: AvroRuntimeException =>
        AvroEventRollupSerializer.logger.warn("Unable to instantiate avro codec with name (" + compressionCodec +
          "). Compression disabled. Exception follows.", e)
    }
    dataFileWriter.get.create(schema.get, out)
  }

  @throws[IOException]
  def flush(): Unit = {
    dataFileWriter.get.flush()
  }

  @throws[IOException]
  def beforeClose(): Unit = {
    // no-op
  }

  def supportsReopen: Boolean = false
}
