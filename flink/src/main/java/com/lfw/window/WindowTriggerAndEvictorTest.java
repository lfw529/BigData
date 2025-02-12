package com.lfw.window;

import com.lfw.pojo.EventBean;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Iterator;

/**
 * 窗口触发器自定义，演示
 * 测试数据：
 * 1,e01,10000,p01,10    [10,20)
 * 1,e02,11000,p02,20
 * 1,e02,12000,p03,40
 * 1,e0x,13000,p03,40    ==> 这里会触发一次窗口
 * 1,e04,16000,p05,50
 * 1,e03,20000,p02,10    [20,30)  ==> 时间到达也会触发一次窗口
 */
public class WindowTriggerAndEvictorTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // 1,e01,3000,pg02
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Tuple2<EventBean, Integer>> beanStream = source.map(s -> {
                    String[] split = s.split(",");
                    EventBean bean = new EventBean(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3], Integer.parseInt(split[4]));
                    return Tuple2.of(bean, 1);
                }).returns(new TypeHint<Tuple2<EventBean, Integer>>() {
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<EventBean, Integer>>forBoundedOutOfOrderness(Duration.ofMillis(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<EventBean, Integer>>() {
                            @Override
                            public long extractTimestamp(Tuple2<EventBean, Integer> element, long recordTimestamp) {
                                return element.f0.getTimeStamp();
                            }
                        }));

        OutputTag<Tuple2<EventBean, Integer>> lateDataOutputTag = new OutputTag<>("late_data", TypeInformation.of(new TypeHint<Tuple2<EventBean, Integer>>() {
        }));

        SingleOutputStreamOperator<String> sumResult = beanStream.keyBy(tp -> tp.f0.getGuid())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))   //事件时间滚动窗口，窗口长度为10
//                .allowedLateness(Time.seconds(2))   //允许迟到2s
//                .sideOutputLateData(lateDataOutputTag)  //迟到超过允许时限的数据，输出到该 "outputTag" 所标记的侧流
                // 设置自定义的Trigger
                .trigger(MyEventTimeTrigger.create())
                // 设置自定义的Evictor，它会在窗口触发计算前，对窗口中的 e0x标记事件进行移除
                .evictor(MyTimeEvictor.of(Time.seconds(10)))
                .apply(new WindowFunction<Tuple2<EventBean, Integer>, String, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<Tuple2<EventBean, Integer>> input, Collector<String> out) throws Exception {
                        int count = 0;
                        for (Tuple2<EventBean, Integer> eventBeanIntegerTuple2 : input) {
                            count++;
                        }
                        out.collect(window.getStart() + ":" + window.getEnd() + "," + count);
                    }
                });

        DataStream<Tuple2<EventBean, Integer>> lateDataSideStream = sumResult.getSideOutput(lateDataOutputTag);

        sumResult.print("主流结果");

        lateDataSideStream.print("迟到数据");

        env.execute();
    }
}

class MyEventTimeTrigger extends Trigger<Tuple2<EventBean, Integer>, TimeWindow> {

    private MyEventTimeTrigger() {
    }

    /**
     * 来一条数据时，需要检查 watermark 是否已经越过窗口结束点需要触发
     */
    @Override
    public TriggerResult onElement(Tuple2<EventBean, Integer> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        //如果窗口结束点 <= 当前的 watermark
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            return TriggerResult.FIRE;
        } else {
            //注册定时器，定时器的触发时间为：窗口的结束点时间
            ctx.registerEventTimeTimer(window.maxTimestamp());

            //判断：当前数据的用户行为事件 id 是否等于 ex0, 如是，则触发
            if ("e0x".equals(element.f0.getEventId()))
                return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    /**
     * 当事件时间定时器的触发时间 (窗口的结束点时间) 到达了，检查是否满足触发条件
     * 下面的方法，是定时器在调用
     */
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }

    /**
     * 当处理时间定时器的触发时间 (窗口的结束点时间) 到达了，检查是否满足触发条件
     */
    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) {
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }
    }

    public static MyEventTimeTrigger create() {
        return new MyEventTimeTrigger();
    }
}

class MyTimeEvictor implements Evictor<Object, TimeWindow> {

    private static final long serialVersionUID = 1L;

    private final long windowSize;
    private final boolean doEvictAfter;

    public MyTimeEvictor(long windowSize) {
        this.windowSize = windowSize;
        this.doEvictAfter = false;
    }

    public MyTimeEvictor(long windowSize, boolean doEvictAfter) {
        this.windowSize = windowSize;
        this.doEvictAfter = doEvictAfter;
    }

    /**
     * 窗口触发前，调用
     */
    @Override
    public void evictBefore(Iterable<TimestampedValue<Object>> elements, int size, TimeWindow window, EvictorContext ctx) {
        if (!doEvictAfter) {
            evict(elements, size, ctx);
        }
    }

    /**
     * 窗口触发后，调用
     */
    @Override
    public void evictAfter(Iterable<TimestampedValue<Object>> elements, int size, TimeWindow window, EvictorContext ctx) {
        if (doEvictAfter) {
            evict(elements, size, ctx);
        }
    }

    /**
     * 元素移除的核心逻辑
     */
    private void evict(Iterable<TimestampedValue<Object>> elements, int size, EvictorContext ctx) {
        if (!hasTimestamp(elements)) {
            return;
        }

        long currentTime = getMaxTimestamp(elements);
        long evictCutoff = currentTime - windowSize;

        for (Iterator<TimestampedValue<Object>> iterator = elements.iterator(); iterator.hasNext(); ) {
            TimestampedValue<Object> record = iterator.next();

            Tuple2<EventBean, Integer> tuple = (Tuple2<EventBean, Integer>) record.getValue();

            //加了一个条件：数据的 eventId = e0x,也移除
            if (record.getTimestamp() <= evictCutoff || tuple.f0.getEventId().equals("e0x")) {
                iterator.remove();
            }
        }
    }

    private boolean hasTimestamp(Iterable<TimestampedValue<Object>> elements) {
        Iterator<TimestampedValue<Object>> it = elements.iterator();
        if (it.hasNext()) {
            return it.next().hasTimestamp();
        }
        return false;
    }

    /**
     * 用于计算移除的时间截止点
     */
    private long getMaxTimestamp(Iterable<TimestampedValue<Object>> elements) {
        long currentTime = Long.MIN_VALUE;
        for (Iterator<TimestampedValue<Object>> iterator = elements.iterator(); iterator.hasNext(); ) {
            TimestampedValue<Object> record = iterator.next();
            currentTime = Math.max(currentTime, record.getTimestamp());
        }
        return currentTime;
    }

    public static MyTimeEvictor of(Time windowSize) {
        return new MyTimeEvictor(windowSize.toMilliseconds());
    }
}