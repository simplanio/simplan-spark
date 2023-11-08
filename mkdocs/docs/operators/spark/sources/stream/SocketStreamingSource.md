# Socket Stream Reader

Reads records from a Socket Stream and converts each record into a Structured Record. The schema of the Structured Record can be provided using the schema property. Schema is provided as [Schema Qualifed Param](../../../../qualified_param/schema_qualified_param.md).

```hocon

    ReadStream {
      action {
        operator = com.intuit.data.simplan.spark.core.operators.sources.stream.SocketStreamingSource
        config = {
          format = <Json, Avro, Protobuf >
          payloadSchema = /path/to/payloadschemafile
          headerSchema = /path/to/headerschemafile
          parseMode = ALL_PARSED
          options = {
            <options1> = <value1>
          }
        }
      }
    }

```

`Format` = to define serialization format. It can be either json, avro or protobuf  
`payloadSchema` = path to your schema file  
`headerSchema` = schema for your header file  
`parseMode` =  different kind of parsing options

Example for reading from socket stream

```hocon
    ReadStream {
      action {
        operator = com.intuit.data.simplan.spark.core.operators.sources.stream.SocketStreamingSource
        config = {
          format = JSON
          //payloadSchema = schemaDDL("someNumber LONG, anotherNumber LONG")
          parseMode = COMPLETE
          options = {
            host = localhost
            port = 12345
          }
        }
      }
    }
```