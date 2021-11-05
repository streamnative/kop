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

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.kafka.common.protocol.Errors;

/**
 * Errors and data.
 */
@Data
@AllArgsConstructor
public class ErrorsAndData<T> {

    private Errors errors;
    private T data;

    public ErrorsAndData() {

    }

    public ErrorsAndData(Errors errors) {
        this.errors = errors;
        this.data = null;
    }

    public ErrorsAndData(T data) {
        this.data = data;
    }

    public boolean hasErrors() {
        return errors != null && errors != Errors.NONE;
    }

}
