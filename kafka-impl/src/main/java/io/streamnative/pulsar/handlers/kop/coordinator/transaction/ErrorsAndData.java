package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

import lombok.Data;
import org.apache.kafka.common.protocol.Errors;

import java.util.Optional;

@Data
public class ErrorsAndData<T> {

    private Errors errors;
    private T data;

    public ErrorsAndData() {

    }

    public ErrorsAndData(Errors errors) {
        this.errors = errors;
        this.data = null;
    }

    public ErrorsAndData(T data) {
        this.data = data;
    }

    public boolean hasErrors() {
        return errors != null && errors != Errors.NONE;
    }

}
