package org.kiji.express.flow.serialization

import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.InputStream
import java.io.OutputStream

import org.apache.avro.Schema
import org.apache.hadoop.io.serializer.Serialization
import org.apache.hadoop.io.serializer.Serializer
import org.apache.hadoop.io.serializer.Deserializer

/**
 * Provides serialization for [[org.apache.avro.Schema]] objects.
 */
class SchemaSerialization extends Serialization[Schema] {

  println("Creating SchemaSerialization obj" + "*"*30)

  class SchemaSerializer extends Serializer[Schema] {
    private var stream: DataOutputStream = null

    override def open(out: OutputStream): Unit = out match {
      case dos: DataOutputStream => stream = dos
      case _ => stream = new DataOutputStream(out)
    }

    override def serialize(schema: Schema) = {
      println("Serializing Schema: " + schema)
      stream.writeUTF(schema.toString(false))
    }

    override def close() = { stream.close() }
  }

  class SchemaDeserializer extends Deserializer[Schema] {
    private var stream: DataInputStream = null
    private val parser: Schema.Parser = new Schema.Parser

    override def close() { stream.close() }

    override def open(in: InputStream) = in match {
      case dis: DataInputStream => stream = dis
      case _ => stream = new DataInputStream(in)
    }

    override def deserialize(t: Schema): Schema = {
      val s = parser.parse(stream.readUTF())
      println("Deserializing Schema: " + s)
      s
    }
  }

  override def accept(c: Class[_]): Boolean = c.isAssignableFrom(classOf[Schema])

  override def getSerializer(c: Class[Schema]): Serializer[Schema] = new SchemaSerializer

  override def getDeserializer(c: Class[Schema]): Deserializer[Schema] = new SchemaDeserializer
}
