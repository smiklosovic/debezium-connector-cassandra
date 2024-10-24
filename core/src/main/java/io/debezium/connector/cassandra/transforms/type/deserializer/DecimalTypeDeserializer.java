/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import static io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders.DOUBLE_TYPE;
import static io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders.STRING_TYPE;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer.DecimalMode;
import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;

public class DecimalTypeDeserializer extends LogicalTypeDeserializer {

    private final DebeziumTypeDeserializer deserializer;
    private final Schema schema;
    private DecimalMode mode;

    public DecimalTypeDeserializer(DebeziumTypeDeserializer deserializer) {
        this.deserializer = deserializer;
        this.schema = VariableScaleDecimal.builder().build();
        this.mode = DecimalMode.DOUBLE;
    }

    @Override
    public Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        Object value = deserializer.deserialize(abstractType, bb);
        return formatDeserializedValue(abstractType, value);
    }

    @Override
    public SchemaBuilder getSchemaBuilder(AbstractType<?> abstractType) {
        switch (mode) {
            case DOUBLE:
                return DOUBLE_TYPE;
            case PRECISE:
                return VariableScaleDecimal.builder();
            case STRING:
                return STRING_TYPE;
        }
        throw new IllegalArgumentException("Unknown decimalHandlingMode");
    }

    @Override
    public Object formatDeserializedValue(AbstractType<?> abstractType, Object value) {
        BigDecimal decimal = (BigDecimal) value;
        switch (mode) {
            case DOUBLE:
                return decimal.doubleValue();
            case PRECISE:
                return VariableScaleDecimal.fromLogical(schema, new SpecialValueDecimal(decimal));
            case STRING:
                return decimal.toPlainString();
        }
        throw new IllegalArgumentException("Unknown decimalHandlingMode");
    }

    public void setMode(DecimalMode mode) {
        this.mode = mode;
    }
}
