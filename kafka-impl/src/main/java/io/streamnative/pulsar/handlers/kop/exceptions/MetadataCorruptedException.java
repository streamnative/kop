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
package io.streamnative.pulsar.handlers.kop.exceptions;

/**
 * Exception is thrown when message metadata is not found in KoP.
 */
public class MetadataCorruptedException extends KoPBaseException {
    private static final long serialVersionUID = 0L;

    public MetadataCorruptedException(String message) {
        super(message);
    }

    public static class NoBrokerEntryMetadata extends MetadataCorruptedException {

        public NoBrokerEntryMetadata() {
            super("No BrokerEntryMetadata found");
        }
    }
}
