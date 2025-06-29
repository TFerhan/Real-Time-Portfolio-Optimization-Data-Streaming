package com.popupd.util;

import com.popupd.util.WeightStockSchema;
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
import java.io.IOException;

public class WeightDeserializationSchema implements KafkaRecordDeserializationSchema<WeightSchema>, ResultTypeQueryable<WeightSchema> {


    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<WeightSchema> collector) throws IOException {
        WeightSchema value = null;
        try{
            value = deserializeFromAvro(consumerRecord.value());

        }catch (Exception e){
            throw new RuntimeException(e);
        }
        collector.collect(value);
    }

    private WeightSchema deserializeFromAvro(byte[] value) throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(value);
        DatumReader<WeightSchema> datum = new SpecificDatumReader<>(WeightSchema.class);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        return datum.read(null, decoder);
    }


    @Override
    public TypeInformation<WeightSchema> getProducedType() {
        return null;
    }
}
