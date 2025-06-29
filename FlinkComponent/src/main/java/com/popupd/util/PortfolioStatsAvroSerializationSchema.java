package com.popupd.util;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PortfolioStatsAvroSerializationSchema implements SerializationSchema<PortfolioStatsSchema> {
    @Override
    public byte[] serialize(PortfolioStatsSchema portfolioStats) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            DatumWriter<PortfolioStatsSchema> writer = new SpecificDatumWriter<>(PortfolioStatsSchema.class);
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(portfolioStats, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize PortfolioStatsSchema", e);
        }
    }
}
