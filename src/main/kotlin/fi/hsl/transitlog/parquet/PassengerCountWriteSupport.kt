package fi.hsl.transitlog.parquet

import fi.hsl.common.passengercount.proto.PassengerCount
import fi.hsl.common.passengercount.proto.PassengerCount.Count
import fi.hsl.common.passengercount.proto.PassengerCount.DoorCount
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType
import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.*

class PassengerCountWriteSupport(private val messageType: MessageType) : WriteSupport<PassengerCount.Data>() {
    private lateinit var recordConsumer: RecordConsumer

    override fun init(configuration: Configuration?): WriteContext =
        WriteContext(messageType, emptyMap())

    override fun prepareForWrite(recordConsumer: RecordConsumer) {
        this.recordConsumer = recordConsumer
    }

    private fun UUID.toBytes(): ByteArray {
        val bb = ByteBuffer.wrap(ByteArray(16))
        bb.putLong(mostSignificantBits)
        bb.putLong(leastSignificantBits)
        return bb.array()
    }

    private fun <T> writeField(index: Int, fieldName: String, value: T?, valueMapper: ((T) -> Any)? = null) {
        if (value != null) {
            val valueToWrite = if (valueMapper != null) { valueMapper(value) } else { value }

            recordConsumer.startField(fieldName, index)
            when (valueToWrite) {
                is String -> recordConsumer.addBinary(Binary.fromString(valueToWrite))
                is Double -> recordConsumer.addDouble(valueToWrite)
                is Long -> recordConsumer.addLong(valueToWrite)
                is Int -> recordConsumer.addInteger(valueToWrite)
                is ByteArray -> recordConsumer.addBinary(Binary.fromConstantByteArray(valueToWrite))
                is Boolean -> recordConsumer.addBoolean(valueToWrite)
                else -> recordConsumer.addBinary(Binary.fromString(valueToWrite.toString()))
            }
            recordConsumer.endField(fieldName, index)
        }
    }

    override fun write(record: PassengerCount.Data) {
        recordConsumer.startMessage()

        for (i in 0 until messageType.fieldCount) {
            when (val fieldName = messageType.getFieldName(i)) {
                "topic" -> writeField(i, fieldName, record.topic)
                "received_at" -> writeField(i, fieldName, record.receivedAt)
                "desi" -> writeField(i, fieldName, record.payload.desi)
                "dir" -> writeField(i, fieldName, record.payload.dir)
                "oper" -> writeField(i, fieldName, record.payload.oper)
                "veh" -> writeField(i, fieldName, record.payload.veh)
                "tst" -> writeField(i, fieldName, record.payload.tst)
                "tsi" -> writeField(i, fieldName, record.payload.tsi) { it * 1000 } //Tsi is in seconds, but Parquet timestamp only supports milliseconds
                "lat" -> writeField(i, fieldName, record.payload.lat)
                "long" -> writeField(i, fieldName, record.payload.long)
                "odo" -> writeField(i, fieldName, record.payload.odo)
                "oday" -> writeField(i, fieldName, record.payload.oday) { LocalDate.parse(it, DateTimeFormatter.BASIC_ISO_DATE).toEpochDay() }
                "jrn" -> writeField(i, fieldName, record.payload.jrn)
                "line" -> writeField(i, fieldName, record.payload.line)
                "start" -> writeField(i, fieldName, record.payload.start) { LocalTime.parse(it, DateTimeFormatter.ISO_LOCAL_TIME).toSecondOfDay() * 1000 }
                "loc" -> writeField(i, fieldName, record.payload.loc)
                "stop" -> writeField(i, fieldName, record.payload.stop)
                "route" -> writeField(i, fieldName, record.payload.route)
                "count_quality" -> writeField(i, fieldName, record.payload.vehicleCounts.countQuality)
                "vehicle_load" -> writeField(i, fieldName, record.payload.vehicleCounts.vehicleLoad)
                "vehicle_load_ratio" -> writeField(i, fieldName, record.payload.vehicleCounts.vehicleLoadRatio)
                "door_counts" -> {
                    recordConsumer.startField(fieldName, i)
                    recordConsumer.startGroup()

                    recordConsumer.startField("list", 0)

                    record.payload.vehicleCounts.doorCountsList.forEach(::writeDoorCount)

                    recordConsumer.endField("list", 0)

                    recordConsumer.endGroup()
                    recordConsumer.endField(fieldName, i)
                }
            }
        }

        recordConsumer.endMessage()
    }

    //TODO: think about better way to handle field indexes
    private fun writeDoorCount(doorCount: DoorCount) {
        recordConsumer.startGroup()
        recordConsumer.startField("element", 0)
        recordConsumer.startGroup()

        writeField(0, "door", doorCount.door)

        recordConsumer.startField("counts", 1)
        recordConsumer.startGroup()
        recordConsumer.startField("list", 0)

        doorCount.countList.forEach(::writeCount)

        recordConsumer.endField("list", 0)
        recordConsumer.endGroup()
        recordConsumer.endField("counts", 1)

        recordConsumer.endGroup()
        recordConsumer.endField("element", 0)
        recordConsumer.endGroup()
    }

    private fun writeCount(count: Count) {
        recordConsumer.startGroup()
        recordConsumer.startField("element", 0)
        recordConsumer.startGroup()

        writeField(0, "class", count.clazz)
        writeField(1, "in", count.`in`)
        writeField(2, "out", count.out)

        recordConsumer.endGroup()
        recordConsumer.endField("element", 0)
        recordConsumer.endGroup()
    }
}