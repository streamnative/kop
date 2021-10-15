/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

import java.nio.ByteBuffer;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;


/**
 * Transaction log key.
 */
@Data
@AllArgsConstructor
public class TransactionLogKey {

    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;

    private final String transactionId;

    private static final String TXN_ID_FIELD = "transactional_id";

    protected static final Schema SCHEMA_0 =
            new Schema(
                    new Field(TXN_ID_FIELD, Type.STRING, "")
            );

    private static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0
    };

    public static Schema getSchema(short schemaVersion) {
        return SCHEMAS[schemaVersion];
    }

    public byte[] toBytes() {
        return toBytes(HIGHEST_SUPPORTED_VERSION);
    }

    public byte[] toBytes(short schemaVersion) {
        Struct struct = new Struct(getSchema(schemaVersion));
        struct.set(TXN_ID_FIELD, transactionId);

        ByteBuffer byteBuffer = ByteBuffer.allocate(2 /* version */ + struct.sizeOf());
        byteBuffer.putShort(schemaVersion);
        struct.writeTo(byteBuffer);
        return byteBuffer.array();
    }

    public static TransactionLogKey decode(ByteBuffer byteBuffer, short schemaVersion) {
        Schema schema = getSchema(schemaVersion);
        // skip version
        byteBuffer.getShort();
        Struct struct = schema.read(byteBuffer);
        return new TransactionLogKey(struct.getString(TXN_ID_FIELD));
    }

}
