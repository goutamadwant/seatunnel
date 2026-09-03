/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.format.avro;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class SchemaRegistryAwareAvroDeserializationSchemaTest {

    private static final String WRITER_SCHEMA =
            "{\"type\":\"record\",\"name\":\"SeaTunnelRecord\",\"fields\":["
                    + "{\"name\":\"value\",\"type\":[\"null\",\"string\"]}]}";

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"value"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});

    private static final CatalogTable CATALOG_TABLE =
            CatalogTableUtil.getCatalogTable("", "", "", "test", ROW_TYPE);

    @Test
    void shouldDecodeConfluentFramedAvroPayload() throws IOException {
        SeaTunnelRow source = new SeaTunnelRow(new Object[] {"seatunnel"});
        byte[] payload = new AvroSerializationSchema(ROW_TYPE).serialize(source);
        byte[] message = frame(payload, 1);

        SeaTunnelRow result =
                new SchemaRegistryAwareAvroDeserializationSchema(CATALOG_TABLE, WRITER_SCHEMA)
                        .deserialize(message);

        Assertions.assertEquals("seatunnel", result.getField(0));
    }

    @Test
    void shouldTreatSchemaIdAsOpaqueMetadata() throws IOException {
        SeaTunnelRow source = new SeaTunnelRow(new Object[] {"seatunnel"});
        byte[] payload = new AvroSerializationSchema(ROW_TYPE).serialize(source);
        byte[] message = frame(payload, Integer.MAX_VALUE);

        SeaTunnelRow result =
                new SchemaRegistryAwareAvroDeserializationSchema(CATALOG_TABLE, WRITER_SCHEMA)
                        .deserialize(message);

        Assertions.assertEquals("seatunnel", result.getField(0));
    }

    @Test
    void throwsOnTruncatedConfluentHeader() {
        IOException exception =
                Assertions.assertThrows(
                        IOException.class,
                        () ->
                                new SchemaRegistryAwareAvroDeserializationSchema(
                                                CATALOG_TABLE, WRITER_SCHEMA)
                                        .deserialize(new byte[] {0, 0, 0, 0}));

        Assertions.assertTrue(exception.getMessage().contains("expected a 5-byte"));
    }

    @Test
    void throwsOnMissingMessage() {
        IOException exception =
                Assertions.assertThrows(
                        IOException.class,
                        () ->
                                new SchemaRegistryAwareAvroDeserializationSchema(
                                                CATALOG_TABLE, WRITER_SCHEMA)
                                        .deserialize(null));

        Assertions.assertTrue(exception.getMessage().contains("expected a 5-byte"));
    }

    @Test
    void throwsOnInvalidMagicByte() {
        IOException exception =
                Assertions.assertThrows(
                        IOException.class,
                        () ->
                                new SchemaRegistryAwareAvroDeserializationSchema(
                                                CATALOG_TABLE, WRITER_SCHEMA)
                                        .deserialize(new byte[] {1, 0, 0, 0, 1}));

        Assertions.assertTrue(exception.getMessage().contains("expected magic byte 0 but found 1"));
    }

    private static byte[] frame(byte[] payload, int schemaId) {
        byte[] result = new byte[5 + payload.length];
        result[0] = 0;
        result[1] = (byte) (schemaId >>> 24);
        result[2] = (byte) (schemaId >>> 16);
        result[3] = (byte) (schemaId >>> 8);
        result[4] = (byte) schemaId;
        System.arraycopy(payload, 0, result, 5, payload.length);
        return result;
    }
}
