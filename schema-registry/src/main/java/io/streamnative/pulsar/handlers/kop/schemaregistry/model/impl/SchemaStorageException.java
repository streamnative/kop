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
package io.streamnative.pulsar.handlers.kop.schemaregistry.model.impl;

import io.netty.handler.codec.http.HttpResponseStatus;
import lombok.Getter;

@Getter
public class SchemaStorageException extends Exception {

    private final int httpStatusCode;

    public SchemaStorageException(Throwable cause) {
        super(cause);
        this.httpStatusCode = HttpResponseStatus.INTERNAL_SERVER_ERROR.code();
    }

    public SchemaStorageException(String message) {
        super(message);
        this.httpStatusCode = HttpResponseStatus.INTERNAL_SERVER_ERROR.code();
    }

    public SchemaStorageException(String message, int httpStatusCode) {
        super(message);
        this.httpStatusCode = httpStatusCode;
    }
}
