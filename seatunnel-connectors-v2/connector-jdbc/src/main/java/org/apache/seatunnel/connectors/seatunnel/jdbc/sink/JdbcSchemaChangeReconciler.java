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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.RestoreTableSchemaEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventDispatcher;
import org.apache.seatunnel.api.table.type.SqlType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Reconciles a receiver against the unique applied prefix of a committed DDL block. */
final class JdbcSchemaChangeReconciler {
    interface SchemaReader {
        TableSchema read() throws Exception;
    }

    interface SchemaApplier {
        void apply(SchemaChangeEvent event) throws Exception;
    }

    static void reconcile(
            TableSchema before,
            TableSchema after,
            List<SchemaChangeEvent> events,
            SchemaReader reader,
            SchemaApplier applier)
            throws IOException {
        reconcile(before, after, events, reader, applier, "");
    }

    static void reconcile(
            TableSchema before,
            TableSchema after,
            List<SchemaChangeEvent> events,
            SchemaReader reader,
            SchemaApplier applier,
            String dialect)
            throws IOException {
        List<SchemaChangeEvent> flattened = new ArrayList<>();
        for (SchemaChangeEvent event : events) {
            if (event instanceof AlterTableColumnsEvent) {
                flattened.addAll(((AlterTableColumnsEvent) event).getEvents());
            } else flattened.add(event);
        }
        events = flattened;
        List<TableSchema> prefixes = new ArrayList<>();
        prefixes.add(before);
        TableSchema schema = before;
        for (SchemaChangeEvent event : events) {
            if (event instanceof RestoreTableSchemaEvent) {
                throw new IOException("A local schema restore cannot apply physical DDL");
            }
            schema = new TableSchemaChangeEventDispatcher().reset(schema).apply(event);
            prefixes.add(schema);
        }
        if (!sameDeclaredSchema(schema, after))
            throw new IOException("Schema change block does not produce the expected sink schema");
        Set<String> columns = new HashSet<>();
        for (TableSchema prefix : prefixes) {
            prefix.getColumns().forEach(column -> columns.add(column.getName()));
        }
        try {
            TableSchema actual = reader.read();
            int applied = -1;
            for (int i = 0; i < prefixes.size(); i++) {
                if (!matches(actual, prefixes.get(i), columns, dialect)) continue;
                if (applied >= 0)
                    throw new IOException("Ambiguous physical JDBC schema change prefix");
                applied = i;
            }
            if (applied < 0)
                throw new IOException(
                        "Physical JDBC schema does not match the committed change block");
            for (int i = applied; i < events.size(); i++) {
                applier.apply(events.get(i));
                if (!matches(reader.read(), prefixes.get(i + 1), columns, dialect)) {
                    throw new IOException(
                            "Physical JDBC schema did not reach the expected change prefix");
                }
            }
        } catch (IOException error) {
            throw error;
        } catch (Exception error) {
            throw new IOException("JDBC schema reconciliation failed", error);
        }
    }

    private static boolean matches(
            TableSchema actual, TableSchema expected, Set<String> columns, String dialect) {
        // A physical table can also contain nullable columns owned by a sibling source.
        // Match the union of this block's before/after names, including names absent at a
        // particular prefix, so ADD/DROP/RENAME cannot be mistaken for an applied change.
        List<Column> left =
                actual.getColumns().stream()
                        .filter(column -> columns.contains(column.getName()))
                        .collect(Collectors.toList());
        List<Column> right = expected.getColumns();
        java.util.Map<String, Column> actualColumns = new java.util.HashMap<>();
        for (Column column : left) actualColumns.put(column.getName(), column);
        if (left.size() != right.size()) return false;
        for (int i = 0; i < left.size(); i++) {
            Column b = right.get(i), a = actualColumns.get(b.getName());
            if (a == null) return false;
            if (!a.getName().equals(b.getName())
                    || !a.getDataType().equals(b.getDataType())
                    || a.isNullable() != b.isNullable()) return false;
            SqlType type = b.getDataType().getSqlType();
            if ((type == SqlType.STRING || type == SqlType.BYTES)
                    && b.getColumnLength() != null
                    && !sameStringLength(a, b, dialect)) return false;
            if (b.getScale() != null && !Objects.equals(a.getScale(), b.getScale())) return false;
            if (!Objects.equals(defaultText(a, dialect), defaultText(b, dialect))
                    || !Objects.equals(text(a.getComment()), text(b.getComment()))) return false;
        }
        return true;
    }

    private static boolean sameDeclaredSchema(TableSchema left, TableSchema right) {
        if (left.getColumns().size() != right.getColumns().size()
                || !Objects.equals(left.getPrimaryKey(), right.getPrimaryKey())
                || !Objects.equals(left.getConstraintKeys(), right.getConstraintKeys()))
            return false;
        java.util.Map<String, Column> columns = new java.util.HashMap<>();
        for (Column column : left.getColumns()) {
            if (columns.put(column.getName(), column) != null) return false;
        }
        for (Column column : right.getColumns()) {
            if (!column.equals(columns.remove(column.getName()))) return false;
        }
        return columns.isEmpty();
    }

    private static boolean sameStringLength(Column actual, Column expected, String dialect) {
        if ("mysql".equalsIgnoreCase(dialect)) {
            String actualDomain = mysqlValueDomain(actual.getSourceType());
            String expectedDomain = mysqlValueDomain(expected.getSourceType());
            if (actualDomain != null || expectedDomain != null)
                return Objects.equals(actualDomain, expectedDomain);
        }
        if ("mysql".equalsIgnoreCase(dialect)
                && actual.getSourceType() != null
                && expected.getSourceType() != null) {
            java.util.regex.Pattern character =
                    java.util.regex.Pattern.compile(
                            "(?i)^(VARCHAR|CHAR)\\s*\\(\\s*([0-9]+)\\s*\\).*$");
            java.util.regex.Matcher a = character.matcher(actual.getSourceType().trim());
            java.util.regex.Matcher b = character.matcher(expected.getSourceType().trim());
            if (a.matches() && b.matches()) {
                // Debezium's byte capacity assumes utf8mb4; MySQL catalog byte capacity
                // reflects the receiver charset. Compare declared character capacity instead.
                return a.group(1).equalsIgnoreCase(b.group(1)) && a.group(2).equals(b.group(2));
            }
        }
        return Objects.equals(actual.getColumnLength(), expected.getColumnLength());
    }

    private static String mysqlValueDomain(String sourceType) {
        if (sourceType == null) return null;
        String type = sourceType.trim();
        if (!type.regionMatches(true, 0, "ENUM", 0, 4) && !type.regionMatches(true, 0, "SET", 0, 3))
            return null;
        int open = type.indexOf('(');
        if (open < 0 || !type.endsWith(")"))
            throw new IllegalArgumentException("MySQL value domain metadata is incomplete");
        StringBuilder normalized =
                new StringBuilder(
                        type.substring(0, open).trim().toUpperCase(java.util.Locale.ROOT));
        boolean quoted = false, escaped = false;
        for (int i = open; i < type.length(); i++) {
            char character = type.charAt(i);
            if (quoted || !Character.isWhitespace(character)) normalized.append(character);
            if (escaped) escaped = false;
            else if (quoted && character == 92) escaped = true;
            else if (character == 39) quoted = !quoted;
        }
        return normalized.toString();
    }

    private static String defaultText(Column column, String dialect) {
        Object value = column.getDefaultValue();
        if (value == null) return null;
        String text = value.toString();
        if ("mysql".equalsIgnoreCase(dialect)
                && column.getDataType().getSqlType() == SqlType.BOOLEAN) {
            if ("true".equalsIgnoreCase(text) || "1".equals(text)) return "1";
            if ("false".equalsIgnoreCase(text) || "0".equals(text)) return "0";
        }
        if ("mysql".equalsIgnoreCase(dialect)
                && (column.getDataType().getSqlType() == SqlType.TIMESTAMP
                        || column.getDataType().getSqlType() == SqlType.TIMESTAMP_TZ)) {
            java.util.regex.Matcher expression =
                    java.util.regex.Pattern.compile(
                                    "(?i)^CURRENT_TIMESTAMP(?:\\(\\s*([0-6]?)\\s*\\))?$")
                            .matcher(text.trim());
            if (expression.matches()) {
                String precision = expression.group(1);
                return "CURRENT_TIMESTAMP("
                        + (precision == null || precision.isEmpty() ? "0" : precision)
                        + ")";
            }
        }
        return text;
    }

    private static String text(Object value) {
        return value == null ? "" : value.toString();
    }

    private JdbcSchemaChangeReconciler() {}
}
