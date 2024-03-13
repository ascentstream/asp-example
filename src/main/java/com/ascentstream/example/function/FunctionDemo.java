package com.ascentstream.example.function;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

public class FunctionDemo implements Function<String, String> {

    @Override
    public String process(String input, Context context) throws Exception {
        return String.format("out-%s!", input);
    }
}
