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
package io.streamnative.pulsar.handlers.kop.utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import io.streamnative.pulsar.handlers.kop.AdminManager;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import java.util.Locale;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.config.SaslConfigs;

public class AdminClientUtils {

    public static AdminClient getKafkaAdminClient(final KafkaServiceConfiguration kafkaConfig,
                                                  final String host,
                                                  final int port) {
        final Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.CLIENT_ID_CONFIG, AdminManager.INTER_ADMIN_CLIENT_ID);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":" + port);
        addSslConfigs(kafkaConfig, adminProps);
        return KafkaAdminClient.create(adminProps);
    }

    public static void addSslConfigs(final KafkaServiceConfiguration kafkaConfig,
                                     final Properties adminProps) {
        adminProps.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, kafkaConfig.getInterBrokerSecurityProtocol());
        final String mechanismInterBrokerProtocol = kafkaConfig.getSaslMechanismInterBrokerProtocol();
        adminProps.put(SaslConfigs.SASL_MECHANISM, mechanismInterBrokerProtocol);
        final String interBrokerSecurityProtocol = kafkaConfig.getInterBrokerSecurityProtocol();
        final String kafkaUser = kafkaConfig.getKafkaTenant() + "/" + kafkaConfig.getKafkaNamespace();
        final String brokerClientAuthenticationParameters = kafkaConfig.getBrokerClientAuthenticationParameters();
        final boolean hasSsl = interBrokerSecurityProtocol.toUpperCase(Locale.ROOT).contains("SSL");
        final boolean hasSasl = interBrokerSecurityProtocol.toUpperCase(Locale.ROOT).contains("SASL");

        if (hasSsl) {
            adminProps.put("ssl.truststore.location", kafkaConfig.getKopSslKeystoreLocation());
            adminProps.put("ssl.truststore.password", kafkaConfig.getKopSslKeystorePassword());
            adminProps.put("ssl.endpoint.identification.algorithm", "");
        }

        if ("PLAIN".equals(mechanismInterBrokerProtocol.toUpperCase(Locale.ROOT)) && !hasSsl && hasSasl) {
            String token = "";
            try {
                // brokerClientAuthenticationParameters={"token":"<token-of-super-user-role>"}
                final JsonObject jsonObject = parseJsonObject(brokerClientAuthenticationParameters);
                token = "token:" + jsonObject.get("token").getAsString();
            } catch (JsonSyntaxException | IllegalStateException e) {
                // brokerClientAuthenticationParameters=token:<token-of-super-user-role>
                token = brokerClientAuthenticationParameters;
            }
            final String plainAuth =
                    String.format("username=\"%s\" password=\"%s\";", kafkaUser, token);
            adminProps.put("sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required " + plainAuth);
        } else if ("OAUTHBEARER".equals(mechanismInterBrokerProtocol.toUpperCase(Locale.ROOT))) {
            adminProps.put("sasl.login.callback.handler.class",
                    "io.streamnative.pulsar.handlers.kop.security.oauth.OauthLoginCallbackHandler");
            final JsonObject jsonObject = parseJsonObject(brokerClientAuthenticationParameters);
            final String issuerUrl = jsonObject.get("issuerUrl").getAsString();
            final String privateKey = jsonObject.get("privateKey").getAsString();
            final String audience = jsonObject.get("audience").getAsString();
            final String oauth =
                    String.format("oauth.issuer.url=\"%s\" oauth.credentials.url=\"%s\" oauth.audience=\"%s\";",
                            issuerUrl, privateKey, audience);
            adminProps.put("sasl.jaas.config",
                    "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " + oauth);
        }
    }

    public static JsonObject parseJsonObject(final String info) {
        JsonParser parser = new JsonParser();
        return parser.parse(info).getAsJsonObject();
    }
}
