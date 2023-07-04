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
package io.streamnative.pulsar.handlers.kop.security.auth;

import java.util.HashMap;
import java.util.Locale;

/**
 * Represents a type of resource which an ACL can be applied to.
 *
 * Current only support topic.
 */
public enum ResourceType {


    /**
     * Represents any ResourceType which this client cannot understand,
     * perhaps because this client is too old.
     */
    UNKNOWN((byte) 0),

    /**
     * A Pulsar topic.
     */
    TOPIC((byte) 1),


    /**
     * A Pulsar Namespace.
     */
    NAMESPACE((byte) 2),

    /**
     * A Pulsar tenant.
     */
    TENANT((byte) 3),

    /**
     * A consumer group.
     */
    GROUP((byte) 4),
    ;


    private final byte code;

    private static final HashMap<Byte, ResourceType> CODE_TO_VALUE = new HashMap<>();

    static {
        for (ResourceType resourceType : ResourceType.values()) {
            CODE_TO_VALUE.put(resourceType.code, resourceType);
        }
    }

    /**
     * Parse the given string as an ACL resource type.
     *
     * @param str The string to parse.
     * @return The ResourceType, or UNKNOWN if the string could not be matched.
     */
    public static ResourceType fromString(String str) {
        try {
            return ResourceType.valueOf(str.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }

    /**
     * Return the ResourceType with the provided code or `ResourceType.UNKNOWN` if one cannot be found.
     */
    public static ResourceType fromCode(byte code) {
        ResourceType resourceType = CODE_TO_VALUE.get(code);
        if (resourceType == null) {
            return UNKNOWN;
        }
        return resourceType;
    }

    ResourceType(byte code) {
        this.code = code;
    }

    /**
     * Return the code of this resource.
     */
    public byte code() {
        return code;
    }

    /**
     * Return whether this resource type is UNKNOWN.
     */
    public boolean isUnknown() {
        return this == UNKNOWN;
    }
}
