package fi.hsl.transitlog.parquet

import fi.hsl.common.passengercount.proto.PassengerCount
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.api.WriteSupport
import java.nio.file.Path

class PassengerCountParquetWriterBuilder(outputFile: Path) : ParquetWriter.Builder<PassengerCount.Data, PassengerCountParquetWriterBuilder>(org.apache.hadoop.fs.Path(outputFile.toUri())) {
    companion object {
        private val PASSENGER_COUNT_SCHEMA by lazy { ParquetSchemaParser.parseSchemaFromResource("apc_parquet_schema.txt") }
    }

    override fun self(): PassengerCountParquetWriterBuilder = this

    override fun getWriteSupport(conf: Configuration?): WriteSupport<PassengerCount.Data> = PassengerCountWriteSupport(PASSENGER_COUNT_SCHEMA)
}