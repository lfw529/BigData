package com.lfw.operator.source;

import com.lfw.pojo.EventLog;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;

/**
 * 自定义 source
 * 可以实现   SourceFunction 或者 RichSourceFunction, 这两者都是非并行的source算子
 * 也可实现   ParallelSourceFunction 或者 RichParallelSourceFunction, 这两者都是可并行的source算子
 * <p>
 * -- 带 Rich 的，都拥有 open() ,close() ,getRuntimeContext() 方法
 * -- 带 Parallel的，都可多实例并行执行
 **/
public class MySourceFunction implements SourceFunction<EventLog> {
    volatile boolean flag = true;

    @Override
    public void run(SourceContext<EventLog> ctx) throws Exception {

        EventLog eventLog = new EventLog();
        String[] events = {"appLaunch", "pageLoad", "adShow", "adClick", "itemShare", "itemCollect", "putBack", "wakeUp", "appClose"};
        HashMap<String, String> eventInfoMap = new HashMap<>();

        while (flag) {
            eventLog.setGuid(RandomUtils.nextLong(1, 1000));
            eventLog.setSessionId(RandomStringUtils.randomAlphabetic(12).toUpperCase());
            eventLog.setTimeStamp(System.currentTimeMillis());
            eventLog.setEventId(events[RandomUtils.nextInt(0, events.length)]);

            eventInfoMap.put(RandomStringUtils.randomAlphabetic(1), RandomStringUtils.randomAlphabetic(2));
            eventLog.setEventInfo(eventInfoMap);

            ctx.collect(eventLog);

            eventInfoMap.clear();

            Thread.sleep(RandomUtils.nextInt(200, 1500));
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}

