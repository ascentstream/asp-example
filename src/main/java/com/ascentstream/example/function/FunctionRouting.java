package com.ascentstream.example.function;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

public class FunctionRouting implements Function<String, CompletableFuture<Void>> {

    @Override
    public CompletableFuture<Void> process(String input, Context context) throws Exception {
        CompletableFuture<?>[] futures = new CompletableFuture<?>[1];
        CompletableFuture<MessageId> sendAsync =
                context.newOutputMessage("new-topic", Schema.STRING)
                        .value(input).sendAsync();
        futures[0] = sendAsync;
        return CompletableFuture.allOf(futures);
    }
}
