package multthreading.producer.consumer.avroschema;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class AvroSchemaRegistry {

  public static GenericRecord createSchema(){
    // CREATE SCHEMA
    String schema = "{\"type\":\"record\"," +
        "\"name\":\"myrecord\"," +
        "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";

    // INITIALIZE SCHEMA PARSER
    Schema.Parser parser = new Parser();
    // PASS SCHEMA TO SCHEMA PARSER AND OBTAIN PARSED SCHEMA
    Schema parsedSchema = parser.parse(schema);
    // INITIALIZE GENERIC RECORD
    GenericRecord avroRecord = new GenericData.Record(parsedSchema);
    // RETURN GENERIC RECORD
    return avroRecord;
  }



}
