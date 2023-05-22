package com.lfw.source;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.SystemClock;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * 能够记录读取位置偏移量的自定义 source
 */
public class HoldOffsetSource extends AbstractSource implements EventDrivenSource, Configurable {
    private static final Logger logger = LoggerFactory.getLogger(HoldOffsetSource.class);

    private String positionFilePath;
    private String logfile;
    private int batchSize;

    private ExecutorService exec;

    /**
     * 框架调用本方法，开始采集数据
     * 自定义代码去读取数据，转为 event
     * 用 getChannelProcessor()方法 (定义在父类中)去获取框架的 channelprocessor(channel 处理器)
     * 调用这个 channelprocessor 将 event 提交给 channel
     */
    @Override
    public synchronized void start() {
        super.start();

        //用于向 channel 提交数据的一个处理器
        ChannelProcessor channelProcessor = getChannelProcessor();

        //获取历史偏移量 [注意：这里的偏移量代表一个字符]
        long offset = 0;
        try {
            //先读取已存在的偏移量
            File positionFile = new File(this.positionFilePath);
            String s = FileUtils.readFileToString(positionFile);
            offset = Long.parseLong(s.trim());
        } catch (IOException e) {
            e.printStackTrace();
        }

        //构造一个线程池
        exec = Executors.newSingleThreadExecutor();
        //向线程池提交数据采集任务
        exec.execute(new HoldOffsetRunnable(offset, logfile, channelProcessor, batchSize, positionFilePath));
    }

    /**
     * 停止前要调用的方法：可以在这里做一些资源关闭清理工作
     */
    @Override
    public synchronized void stop() {
        super.stop();

        try {
            exec.shutdown();
        } catch (Exception e) {
            exec.shutdown();
        }
    }

    /**
     * 获取配置文件中的参数，来配置本source实例
     * 要哪些参数：
     * 偏移量记录文件所在路径
     * 要采集的文件所在路径
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        //这是我们 source 用来记录偏移量的文件路径
        this.positionFilePath = context.getString("positionfile", "./");

        //这是我们 source 要采集的日志文件路径
        this.logfile = context.getString("logfile");

        // 这是用户配置的采集事务批次最大值
        this.batchSize = context.getInteger("batchsize", 100);

        // 如果日志文件路径没有指定，则抛异常
        if (StringUtils.isBlank(logfile)) throw new RuntimeException("请配置需要采集的文件路径");
    }

    /**
     * 采集文件的具体工作线程任务类
     */
    private static class HoldOffsetRunnable implements Runnable {
        long offset;
        String logFilepath;
        String positionFilepath;
        ChannelProcessor channelProcessor;  // channel 提交器 (里面会调拦截器，会开启写入 channel 的事务)
        int batchSize;     //批次大小
        List<Event> events = new ArrayList<Event>();   //用来保存一批事件
        SystemClock systemClock = new SystemClock();

        public HoldOffsetRunnable(long offset, String logFilepath, ChannelProcessor channelProcessor, int batchSize, String positionFilepath) {
            this.offset = offset;
            this.logFilepath = logFilepath;
            this.channelProcessor = channelProcessor;
            this.batchSize = batchSize;
            this.positionFilepath = positionFilepath;
        }

        public void run() {
            try {
                //先定位到指定offset开始读文件
                RandomAccessFile raf = new RandomAccessFile(logFilepath, "r");
                raf.seek(offset);

                //循环读数据
                String line = null;
                //记录上一批提交的时间
                long lastBatchTime = System.currentTimeMillis();
                while (true) {
                    //尝试读取一行
                    line = raf.readLine();
                    //如果此刻，文件中没有新增数据，则等待2s，请继续
                    if (line == null) {
                        Thread.sleep(2000);
                        continue;
                    }

                    //将数据转成 event 写入 channel
                    Event event = EventBuilder.withBody(line.getBytes());
                    //装入 list batch
                    synchronized (HoldOffsetSource.class) {
                        events.add(event);
                    }

                    // 判断批次大小是否满 或者 时间到了没有
                    if (events.size() >= batchSize || timeout(lastBatchTime)) {
                        // 满足，则提交
                        channelProcessor.processEventBatch(events);

                        // 记录提交时间
                        lastBatchTime = systemClock.currentTimeMillis();

                        // 记录偏移量
                        long offset = raf.getFilePointer();
                        FileUtils.writeStringToFile(new File(positionFilepath), offset + "");

                        // 清空本批event
                        events.clear();
                    }
                    //不满足，继续读
                }
            } catch (FileNotFoundException e) {
                logger.error("要采集的文件不存在");
            } catch (IOException e) {
                logger.error("我也不知道怎么搞的，不好意思，我罢工了");
            } catch (InterruptedException e) {
                logger.error("线程休眠出问题了");
            }
        }

        // 判断是否批次间隔超时
        private boolean timeout(long lastBatchTime) {
            return systemClock.currentTimeMillis() - lastBatchTime > 2000;
        }

    }
}
