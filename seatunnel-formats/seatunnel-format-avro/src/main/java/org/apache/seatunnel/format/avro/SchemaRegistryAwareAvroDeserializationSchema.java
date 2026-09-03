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

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;

/** Deserializes Avro payloads prefixed by the Confluent Schema Registry wire-format header. */
public class SchemaRegistryAwareAvroDeserializationSchema
        implements DeserializationSchema<SeaTunnelRow> {

    private static final long serialVersionUID = -1739661689505680032L;

    private static final int HEADER_LENGTH = 5;
    private static final byte MAGIC_BYTE = 0;

    private final AvroDeserializationSchema inner;

    public SchemaRegistryAwareAvroDeserializationSchema(
            CatalogTable catalogTable, String writerSchema) {
        this.inner = new AvroDeserializationSchema(catalogTable, writerSchema);
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        if (message == null || message.length < HEADER_LENGTH) {
            throw new IOException(
                    "Invalid Confluent Avro message: expected a 5-byte Schema Registry header");
        }
        if (message[0] != MAGIC_BYTE) {
            throw new IOException(
                    String.format(
                            "Invalid Confluent Avro message: expected magic byte 0 but found %d",
                            message[0] & 0xFF));
        }

        return inner.deserialize(message, HEADER_LENGTH, message.length - HEADER_LENGTH);
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return inner.getProducedType();
    }
}
