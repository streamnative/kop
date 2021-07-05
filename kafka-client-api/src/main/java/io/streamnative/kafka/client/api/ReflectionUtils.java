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
