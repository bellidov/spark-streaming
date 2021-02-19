package org.example;

import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

import java.util.HashMap;
import java.util.Map;

public class CustomTracer {

    private static HTraceConfiguration configurations() {
        Map<String, String> properties = new HashMap<>();
        properties.put("span.receiver.classes", "org.apache.htrace.impl.ZipkinSpanReceiver");
        properties.put("sampler.classes", "AlwaysSampler");
        properties.put("zipkin.scribe.hostname", "172.18.0.4");
        properties.put("zipkin.scribe.port", "9410");

        HTraceConfiguration hTraceConfiguration = HTraceConfiguration.fromMap(properties);

        return hTraceConfiguration;
    }

    public static void trace(Runnable task) {
        try (Tracer tracer = new Tracer.Builder("SparkTracer")
                .conf(configurations()).build();
             TraceScope ts = tracer.newScope("Parent Scope")) {
            task.run();
        }

        System.out.println("runnable task finished");
    }

}
