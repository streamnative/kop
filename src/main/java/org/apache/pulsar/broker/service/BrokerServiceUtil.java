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
package org.apache.pulsar.broker.service;

/**
 * Util class to access {@link BrokerService}.
 */
public final class BrokerServiceUtil {

    /**
     * Start the stats updater for the given broker service.
     *
     * @param service the broker service
     * @param statsUpdateInitailDelayInSecs initial delay in seconds
     * @param statsUpdateFrequencyInSecs update frequency in seconds
     */
    public static void startStatsUpdater(BrokerService service,
                                         int statsUpdateInitailDelayInSecs,
                                         int statsUpdateFrequencyInSecs) {
        service.startStatsUpdater(statsUpdateInitailDelayInSecs, statsUpdateFrequencyInSecs);
    }

    private BrokerServiceUtil() {}

}
