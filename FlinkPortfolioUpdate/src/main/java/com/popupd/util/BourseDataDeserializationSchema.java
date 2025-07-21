package com.popupd.util;

import com.popupd.util.BourseData;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.util.Collector;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.ByteArrayInputStream;

public class BourseDataDeserializationSchema implements KafkaRecordDeserializationSchema<BourseData>, ResultTypeQueryable<BourseData> {

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<BourseData> out) {
        BourseData value = null;
        try {
            value = deserializeFromAvro(record.value());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        out.collect(value);
    }

    private BourseData deserializeFromAvro(byte[] data) throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        DatumReader<BourseData> reader = new SpecificDatumReader<>(BourseData.class);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        return reader.read(null, decoder);
    }



    @Override
    public TypeInformation<BourseData> getProducedType() {
        return TypeInformation.of(BourseData.class);
    }
}
