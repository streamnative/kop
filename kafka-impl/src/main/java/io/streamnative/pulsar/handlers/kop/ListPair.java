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

import io.streamnative.pulsar.handlers.kop.utils.CoreUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A wrapper of Map<Boolean, List<T>>, which can be returned by {@link java.util.stream.Collectors#groupingBy} with
 * a {@link Function} that returns a Boolean.
 *
 * If we operates on the returned Map directly with {@link Map#get}, a null value might be returned. This class returns
 * a empty map to avoid null check. In addition, the method name is more readable than `get(true)` or `get(false)`.
 *
 * @param <T>
 */
public class ListPair<T> {

    private final Map<Boolean, List<T>> data;

    public <R> ListPair<R> map(final Function<T, R> function) {
        return of(CoreUtils.mapValue(data, list -> CoreUtils.listToList(list, function)));
    }

    public List<T> getSuccessfulList() {
        return Optional.ofNullable(data.get(true)).orElse(Collections.emptyList());
    }

    public List<T> getFailedList() {
        return Optional.ofNullable(data.get(false)).orElse(Collections.emptyList());
    }

    public static <T> ListPair<T> split(final Stream<T> stream,
                                        final Function<T, Boolean> function) {
        return ListPair.of(stream.collect(Collectors.groupingBy(function)));
    }

    public static <T> ListPair<T> of(final Map<Boolean, List<T>> data) {
        return new ListPair<>(data);
    }

    private ListPair(final Map<Boolean, List<T>> data) {
        this.data = (data != null) ? data : Collections.emptyMap();
    }
}
