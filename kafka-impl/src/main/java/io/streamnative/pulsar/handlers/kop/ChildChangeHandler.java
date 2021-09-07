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
package io.streamnative.pulsar.handlers.kop;

public interface ChildChangeHandler {
    String path();

    void handleChildChange();
}

class DeletionTopicsHandler implements ChildChangeHandler {
    private final KopEventManager kopEventManager;

    public DeletionTopicsHandler(KopEventManager kopEventManager) {
        this.kopEventManager = kopEventManager;
    }

    @Override
    public String path() {
        return KopEventManager.getBrokersChangePath();
    }

    @Override
    public void handleChildChange() {
        kopEventManager.put(kopEventManager.getDeleteTopicEvent());
    }
}

class BrokersChangeHandler implements ChildChangeHandler {
    private final KopEventManager kopEventManager;

    public BrokersChangeHandler(KopEventManager kopEventManager) {
        this.kopEventManager = kopEventManager;
    }

    @Override
    public String path() {
        return KopEventManager.getBrokersChangePath();
    }

    @Override
    public void handleChildChange() {
        kopEventManager.put(kopEventManager.getBrokersChangeEvent());
    }

}
