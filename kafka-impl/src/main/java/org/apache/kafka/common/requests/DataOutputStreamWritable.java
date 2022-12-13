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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.utils.Utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class is necessary to bypass a bug in
 * <a href="https://github.com/apache/kafka/blob/927edfece3db8aab7d01850955f9a65e5c110da5/clients/src/main/java/org/apache/kafka/common/protocol/DataOutputStreamWritable.java#L102">DataOutputStreamWritable</a>
 */
public class DataOutputStreamWritable extends org.apache.kafka.common.protocol.DataOutputStreamWritable {
    public DataOutputStreamWritable(DataOutputStream out) {
        super(out);
    }
    @Override
    public void writeByteBuffer(ByteBuffer buf) {
        try {
            if (buf.hasArray()) {
                out.write(buf.array(), buf.arrayOffset(), buf.limit());
            } else {
                byte[] bytes = Utils.toArray(buf);
                out.write(bytes);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
