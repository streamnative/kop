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
package io.streamnative.pulsar.handlers.kop.utils.timer;

import io.streamnative.pulsar.handlers.kop.utils.timer.TimerTaskList.TimerTaskEntry;

/**
 * Timer task.
 */
public abstract class TimerTask implements Runnable {

    protected final long delayMs;
    private TimerTaskEntry timerTaskEntry = null;

    protected TimerTask(long delayMs) {
        this.delayMs = delayMs;
    }

    public synchronized void cancel() {
        if (null != timerTaskEntry) {
            timerTaskEntry.remove();
            timerTaskEntry = null;
        }
    }

    synchronized void setTimerTaskEntry(TimerTaskEntry entry) {
        // if this timerTask is already held by an existing timer task entry,
        // we will remove such an entry first.
        if (null != timerTaskEntry && timerTaskEntry != entry) {
            timerTaskEntry.remove();
        }
        timerTaskEntry = entry;
    }

    synchronized TimerTaskEntry getTimerTaskEntry() {
        return timerTaskEntry;
    }

}
