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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static org.apache.pulsar.common.util.FieldParser.setEmptyValue;
import static org.apache.pulsar.common.util.FieldParser.value;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.configuration.PulsarConfiguration;

/**
 * Configuration Utils.
 */
public final class ConfigurationUtils {

    /**
     * Creates PulsarConfiguration and loads it with populated attribute values loaded from provided property file.
     *
     * @param configFile
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public static <T extends PulsarConfiguration> T create(
            String configFile,
            Class<? extends PulsarConfiguration> clazz) throws IOException, IllegalArgumentException {
        checkNotNull(configFile);
        return create(new FileInputStream(configFile), clazz);
    }

    /**
     * Creates PulsarConfiguration and loads it with populated attribute values loaded from provided inputstream
     * property file.
     *
     * @param inStream
     * @throws IOException
     *             if an error occurred when reading from the input stream.
     * @throws IllegalArgumentException
     *             if the input stream contains incorrect value type
     */
    public static <T extends PulsarConfiguration> T create(
            InputStream inStream,
            Class<? extends PulsarConfiguration> clazz) throws IOException, IllegalArgumentException {
        try {
            checkNotNull(inStream);
            Properties properties = new Properties();
            properties.load(inStream);
            return (create(properties, clazz));
        } finally {
            if (inStream != null) {
                inStream.close();
            }
        }
    }

    /**
     * Creates PulsarConfiguration and loads it with populated attribute values from provided Properties object.
     *
     * @param properties The properties to populate the attributed from
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <T extends PulsarConfiguration> T create(
            Properties properties,
            Class<? extends PulsarConfiguration> clazz) {
        checkNotNull(properties);
        T configuration = null;
        try {
            configuration = (T) clazz.newInstance();
            configuration.setProperties(properties);
            update((Map) properties, configuration);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException("Failed to instantiate " + clazz.getName(), e);
        }
        return configuration;
    }


    /**
     * Update given Object attribute by reading it from provided map properties.
     *
     * @param properties
     *            which key-value pair of properties to assign those values to given object
     * @param obj
     *            object which needs to be updated
     * @throws IllegalArgumentException
     *             if the properties key-value contains incorrect value type
     */
    public static <T> void update(Map<String, String> properties, T obj) throws IllegalArgumentException {
        Field[] fields = obj.getClass().getDeclaredFields();
        // super fields
        Field[] superFields = obj.getClass().getSuperclass().getDeclaredFields();

        Stream.of(fields, superFields).flatMap(Stream::of).forEach(f -> {
            if (properties.containsKey(f.getName())) {
                try {
                    f.setAccessible(true);
                    String v = (String) properties.get(f.getName());
                    if (!StringUtils.isBlank(v)) {
                        f.set(obj, value(v, f));
                    } else {
                        setEmptyValue(v, f, obj);
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException(format("failed to initialize %s field while setting value %s",
                        f.getName(), properties.get(f.getName())), e);
                }
            }
        });
    }

    private ConfigurationUtils() {}

}
