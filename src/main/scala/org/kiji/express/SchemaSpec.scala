package org.kiji.express

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord

/**
 * A specification of how to read or write values to a Kiji column.
 */
sealed trait SchemaSpec extends java.io.Serializable {
  /**
   * Retrieve the Avro [[org.apache.avro.Schema]] object associated with this SchemaSpec,
   * if possible.
   */
  private[express] def schema: Option[Schema]
}

/**
 * Module to provide SchemaSpec implementations.
 */
object SchemaSpec {
  /**
   * Specifies reading or writing with the supplied [[org.apache.avro.Schema]].
   *
   * Note that for serialization reasons this class takes the json form of the schema;
   * use the factory function in the companion class to construct directly with a
   * [[org.apache.avro.Schema]].
   * @param json representation of the supplied schema.
   */
  case class Generic(json: String) extends SchemaSpec {
    @transient override lazy val schema = Some(Generic.parse(json))
  }

  object Generic {
    private def parse(json: String): Schema = new Schema.Parser().parse(json)

    /**
     * Factory function for creating a Generic with an Avro [[org.apache.avro.Schema]].
     * @param schema with which to create [[org.kiji.express.SchemaSpec.Generic]].
     * @return a Generic SchemaSpec with the supplied Schema.
     */
    def apply(schema: Schema) = new Generic(schema.toString(false))
  }

  /**
   * A specification for reading or writing as an instance of the supplied Avro specific record.
   * @param klass of the specific record.
   */
  case class Specific(klass: Class[_ <: SpecificRecord]) extends SchemaSpec {
    @transient override lazy val schema = Some(klass.newInstance.getSchema)
  }

  /**
   * Use the writer schema associated with a value to read or write.
   *
   * In the case of reading a value, the writer schema used to serialize the value will be used.
   * In the case of writing a value, the schema attached to or inferred from the value will be used.
   */
  case object Writer extends SchemaSpec {
    override val schema = None
  }

  /**
   * Use the default reader schema of the column to read or write the values to the column.
   */
  case object DefaultReader extends SchemaSpec {
    override val schema = None
  }
}

