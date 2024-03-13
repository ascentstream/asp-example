package com.ascentstream.example.function;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

public class FunctionLog implements Function<String, String> {

    @Override
    public String process(String input, Context context) throws Exception {
        Logger LOG = context.getLogger();
        LOG.info("this is log {}", input);
        return null;
    }
}