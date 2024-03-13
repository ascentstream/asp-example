package com.ascentstream.example.function;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

public class FunctionMetrics implements Function<String, String> {

    @Override
    public String process(String input, Context context) throws Exception {
        for (String key : input.split("\\.")) {
            context.incrCounter(key, 1);
            context.recordMetric(key, context.getCounter(key));
        }
        return String.format("out-%s", input);
    }
}
