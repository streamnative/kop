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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Reflection Utils.
 */
public final class ReflectionUtils {

    /**
     * Get the private field value from an object's super class.
     *
     * @param privateObject the object
     * @param fieldName the private field name
     * @return the value of the private field
     */
    @SuppressWarnings("unchecked")
    public static <T> T getSuperField(Object privateObject, String fieldName) {
        try {
            Field privateField = privateObject.getClass().getSuperclass().getDeclaredField(fieldName);
            privateField.setAccessible(true);
            return (T) privateField.get(privateObject);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Unable to access field '" + fieldName + "'", e);
        }
    }

    /**
     * Set the private field value for an object's super class.
     *
     * @param privateObject the object
     * @param fieldName the private field name
     * @param fieldValue the private field value
     * @throws IllegalAccessException
     * @throws NoSuchFieldException
     */
    public static <T> void setSuperField(Object privateObject,
                                    String fieldName,
                                    T fieldValue)
            throws IllegalAccessException, NoSuchFieldException {
        Field privateField = privateObject.getClass().getSuperclass().getDeclaredField(fieldName);
        privateField.setAccessible(true);
        privateField.set(privateObject, fieldValue);
    }

    /**
     * Call the private method's super class method.
     *
     * @param privateObject the object
     * @param methodName the private method name
     */
    public static void callSuperNoArgVoidMethod(Object privateObject,
                                           String methodName) throws Exception {
        try {
            Method privateStringMethod = privateObject.getClass().getSuperclass()
                .getDeclaredMethod(methodName, (Class<?>[]) null);

            privateStringMethod.setAccessible(true);
            privateStringMethod.invoke(privateObject, (Object[]) null);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException("Unable to call method '" + methodName + "'", e);
        } catch (InvocationTargetException e) {
            if (null == e.getCause()) {
                throw e;
            } else {
                throw (Exception) e.getCause();
            }
        }
    }

    /**
     * set the private method's accessible.
     *
     * @param privateObject the object
     * @param methodName the private method name
     * @param parameterTypes the parameter types
     */
    public static Method setMethodAccessible(Object privateObject,
                                             String methodName,
                                             Class<?>... parameterTypes) throws Exception {
        Method privateStringMethod = privateObject.getClass().getDeclaredMethod(methodName, parameterTypes);

        privateStringMethod.setAccessible(true);

        return privateStringMethod;
    }

    private ReflectionUtils() {}

}
