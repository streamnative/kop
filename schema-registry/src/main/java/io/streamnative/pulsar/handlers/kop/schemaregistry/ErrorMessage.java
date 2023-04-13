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
package io.streamnative.pulsar.handlers.kop.schemaregistry;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Migrate from io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage.
 */
public class ErrorMessage {

    private final int errorCode;
    private final String message;

    public ErrorMessage(@JsonProperty("error_code") int errorCode, @JsonProperty("message") String message) {
        this.errorCode = errorCode;
        this.message = message;
    }

    @JsonProperty("error_code")
    public int getErrorCode() {
        return this.errorCode;
    }

    @JsonProperty
    public String getMessage() {
        return this.message;
    }
}
