package com.ascentstream.example.function;

import com.ascentstream.example.utils.PulsarUtil;
import java.util.Collection;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.WindowContext;
import org.apache.pulsar.functions.api.WindowFunction;
import org.slf4j.Logger;

public class WordCountFunction implements WindowFunction<String, String> {
    @Override
    public String process(Collection<Record<String>> inputs, WindowContext context) throws Exception {
        Logger Logger = context.getLogger();
        for (Record<String> input : inputs) {
            String key = input.getTopicName().get();
            Logger.info(key);
            Message<String> t = input.getMessage().get();

            System.out.println(t.getMessageId() + ":" + PulsarUtil.getTime(t.getPublishTime()));
        }
        return "key " + inputs.size(); //输出到另外一个Topic里面
    }
}
