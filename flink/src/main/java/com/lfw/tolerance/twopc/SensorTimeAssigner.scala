package com.lfw.tolerance.twopc

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Assigns timestamps to SensorReadings based on their internal timestamp and
 * emits watermarks with five seconds slack.
 */
class SensorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {

  //从SensorReading中提取时间戳。
  override def extractTimestamp(r: SensorReading): Long = r.timestamp
}
