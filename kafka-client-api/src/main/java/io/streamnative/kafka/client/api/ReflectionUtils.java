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
package io.streamnative.kafka.client.api;

import java.lang.reflect.InvocationTargetException;

/**
 * Utils methods for reflection.
 */
public class ReflectionUtils {

    /**
     * Invoke a method of an object but throw an unchecked exception.
     *
     * @param clazz the class type of `object`
     * @param name the method name
     * @param object the object to call the method
     * @param <T> the type of `object`
     * @return the returned value of the method
     */
    public static <T> Object invoke(Class<T> clazz, String name, Object object) {
        try {
            return clazz.getMethod(name).invoke(object);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }
}
