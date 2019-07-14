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
package io.streamnative.kop;


import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.Category;
import org.apache.pulsar.common.configuration.FieldContext;

/**
 * Kafka on Pulsar service configuration object.
 */
@Getter
@Setter
public class KafkaServiceConfiguration extends ServiceConfiguration {

    @Category
    private static final String CATEGORY_KOP = "Kafka on Pulsar";


    //
    // --- Kafka on Pulsar Broker configuration ---
    //

    @FieldContext(
        category = CATEGORY_KOP,
        required = true,
        doc = "Name of the cluster to which this Kafka Broker belongs to"
    )
    private String kafkaClusterName = "kafka";

    @FieldContext(
        category = CATEGORY_KOP,
        required = true,
        doc = "Kafka on Pulsar Broker namespace"
    )
    private String kafkaNamespace = "public/default";

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "The port for serving Kafka requests"
    )

    private Optional<Integer> kafkaServicePort = Optional.of(9902);

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "The port for serving tls secured Kafka requests"
    )
    private Optional<Integer> kafkaServicePortTls = Optional.empty();

}
