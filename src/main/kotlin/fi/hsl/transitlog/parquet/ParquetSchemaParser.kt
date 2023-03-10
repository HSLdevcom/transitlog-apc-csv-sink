package fi.hsl.transitlog.parquet

import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.MessageTypeParser

object ParquetSchemaParser {
    fun parseSchemaFromResource(name: String): MessageType {
        val schemaAsString = ParquetSchemaParser.javaClass.classLoader.getResourceAsStream(name).readAllBytes().decodeToString()

        return MessageTypeParser.parseMessageType(schemaAsString)
    }
}