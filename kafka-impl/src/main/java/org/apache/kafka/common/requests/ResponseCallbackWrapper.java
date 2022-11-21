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

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

/**
 * A wrapper for {@link org.apache.kafka.common.requests.AbstractResponse} that
 * can perform some cleanup tasks after writing to the channel.
 */
public class ResponseCallbackWrapper extends AbstractResponse {

    private AbstractResponse abstractResponse;
    private ResponseCallback responseCallback;

    public ResponseCallbackWrapper(AbstractResponse abstractResponse, ResponseCallback responseCallback) {
        this.abstractResponse = abstractResponse;
        this.responseCallback = responseCallback;
    }

    @VisibleForTesting
    public AbstractResponse getResponse() {
        return this.abstractResponse;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return abstractResponse.errorCounts();
    }

    @Override
    protected Struct toStruct(short i) {
        return abstractResponse.toStruct(i);
    }

    public void responseComplete() {
        responseCallback.responseComplete();
    }
}
