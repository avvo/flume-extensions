package com.avvo.data.flume.extensions

import java.io._
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
  private val logger = LoggerFactory.getLogger(classOf[AvroEventRollupSerializer])

  class Builder extends EventSerializer.Builder {
    def build(context: Context, out: OutputStream): EventSerializer = {
      using(new AvroEventRollupSerializer(out)) {
        writer => {
          writer.configure(context)
          writer
        }
      }
    }
  }
}

class AvroEventRollupSerializer(out: OutputStream) extends EventSerializer with Configurable {
  val AVRO_SCHEMA_PATH: String = "avroSchemaPath"

  var compressionCodec: Option[String] = None
  var dataFileWriter: Option[DataFileWriter[AnyRef]] = None
  var schema: Option[Schema] = None
  var schemaPath: Option[String] = None
  var syncIntervalBytes: Integer = 0
  var reader = new GenericDatumReader[AnyRef]
  var writer: Option[DatumWriter[AnyRef]] = None

  def configure(context: Context): Unit = {
    syncIntervalBytes = context.getInteger(SYNC_INTERVAL_BYTES, DEFAULT_SYNC_INTERVAL_BYTES)
    compressionCodec = Some(context.getString(COMPRESSION_CODEC, DEFAULT_COMPRESSION_CODEC))
    schemaPath = Some(context.getString(AVRO_SCHEMA_PATH))
  }

  @throws[IOException]
  def loadSchema(schemaFilePath: String): Option[Schema] = {
    AvroEventRollupSerializer.logger.debug("Schema Path: " + schemaFilePath)

    val parser = new Schema.Parser()
    if (schemaFilePath.toLowerCase.startsWith("hdfs://")) {
      val fs = FileSystem.get(new Configuration())
      using(fs.open(new Path(schemaFilePath))) { input =>
          Some(parser.parse(input))
      }
    } else {
      throw new FlumeException("Schema file path should begin with HDFS: " + schemaFilePath)
    }
  }

  @throws[IOException]
  def afterCreate() = {
    // no-op
  }

  @throws[IOException]
  def afterReopen(): Unit = {
    // impossible to initialize DataFileWriter without writing the schema?
    throw new UnsupportedOperationException("Avro API doesn't support append")
  }

  @throws[IOException]
  def serialize(value: AnyRef): Array[Byte] = {
    using(new ByteArrayOutputStream()) {
      outputStream => {
        val encoder = EncoderFactory.get.directBinaryEncoder(outputStream, null)
        val writer = new GenericDatumWriter[GenericData.Record](value.asInstanceOf[GenericData.Record].getSchema)
        writer.write(value.asInstanceOf[GenericData.Record], encoder)
        outputStream.toByteArray
      }
    }
  }

  def readAvroData(flumeEventBody: Array[Byte]): Array[Byte] = {
    val inStream = new ByteArrayInputStream(flumeEventBody)
    using(new DataFileStream[AnyRef](inStream, reader)) {
      fileReader => serialize(fileReader.next)
    }
  }

  @throws[IOException]
  private def initialize(event: Event) = {
    if (schemaPath.isEmpty) {
      throw new FlumeException("schemaPath is not initialized.")
    }

    schema = loadSchema(schemaPath.get)
    writer = Some(new GenericDatumWriter[AnyRef](schema.get))

    dataFileWriter = Some(new DataFileWriter(writer.get))
    dataFileWriter.get.setSyncInterval(syncIntervalBytes)

    try {
      val codecFactory = CodecFactory.fromString(compressionCodec.get)
      dataFileWriter.get.setCodec(codecFactory)
    } catch {
      case e: AvroRuntimeException =>
        AvroEventRollupSerializer.logger.warn("Unable to instantiate avro codec with name (" +
          compressionCodec + "). Compression disabled. Exception follows.", e)
    }

    dataFileWriter.get.create(schema.get, out)
  }

  @throws[IOException]
  def write(event: Event): Unit = {
    if (dataFileWriter.isEmpty) {
      initialize(event)
    }
    dataFileWriter.get.appendEncoded(ByteBuffer.wrap(readAvroData(event.getBody)))
  }

  @throws[IOException]
  def flush(): Unit = {
    if (dataFileWriter.isEmpty) {
      throw new FlumeException("dataFileWriter is not initialized yet.")
    }
    dataFileWriter.get.flush()
  }

  @throws[IOException]
  def beforeClose(): Unit = {
    // no-op
  }

  def supportsReopen: Boolean = false
}
