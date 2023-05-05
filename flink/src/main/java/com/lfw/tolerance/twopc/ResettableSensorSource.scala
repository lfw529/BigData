package com.lfw.tolerance.twopc

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import java.util.Calendar
import scala.collection.JavaConverters._
import scala.util.Random

/**
 * A resettable Flink SourceFunction to generate SensorReadings with random temperature values.
 *
 * Each parallel instance of the source simulates 10 sensors which emit one sensor
 * reading every 100 ms.
 *
 * The sink is integrated with Flink's checkpointing mechanism and can be reset to reproduce
 * previously emitted records.
 */

class ResettableSensorSource extends RichParallelSourceFunction[SensorReading] with CheckpointedFunction {

  //指示源是否仍在运行的标志。
  var running: Boolean = true;

  //最后发出的传感器读数
  var readings: Array[SensorReading] = _

  //状态到检查点上次发出的读数
  var sensorsState: ListState[SensorReading] = _

  //run() 通过 SourceContext 发出 SensorReadings，从而连续发出 SensorRead。
  override def run(srcCtx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // initialize random number generator
    val rand = new Random()

    // emit data until being canceled
    while (running) {
      // take a lock to ensure we don't emit while taking a checkpoint
      srcCtx.getCheckpointLock.synchronized {
        // emit readings for all sensors
        for (i <- readings.indices) {
          //get reading
          val reading = readings(i)

          //update timestamp and temperature
          val newTime = reading.timestamp + 100
          //确定性温度生成的设定种子
          rand.setSeed(newTime ^ reading.temperature.toLong)
          val newTemp = reading.temperature + (rand.nextGaussian() * 0.5)
          val newReading = new SensorReading(reading.id, newTime, newTemp)

          // store new reading and emit it
          readings(i) = newReading
          srcCtx.collect(newReading)
        }
      }
      Thread.sleep(100)
    }
  }

  /** Cancels this SourceFunction. */
  override def cancel(): Unit = {
    running = false
  }

  /** 从检查点状态加载以前的读数或生成初始读数。 */
  override def initializeState(ctx: FunctionInitializationContext): Unit = {
    // define state of sink as union list operator state
    this.sensorsState = ctx.getOperatorStateStore.getUnionListState(
      new ListStateDescriptor[SensorReading]("sensorsState", classOf[SensorReading]))

    //获取迭代器状态
    val sensorsStateIt = sensorsState.get().iterator()

    if (!sensorsStateIt.hasNext) {
      val rand = new Random()
      val numTasks = getRuntimeContext.getNumberOfParallelSubtasks
      val thisTask = getRuntimeContext.getIndexOfThisSubtask
      val curTime = Calendar.getInstance().getTimeInMillis

      //initialize sensor ids and temperatures
      this.readings = (0 until 10)
        .map { i =>
          val idx = thisTask + i * numTasks
          val sensorId = s"sensor_$idx"
          val temp = 65 + rand.nextGaussian() * 20
          new SensorReading(sensorId, curTime, temp)
        }.toArray
    } else {
      // select the sensors to handle in this task
      val numTasks = getRuntimeContext.getNumberOfParallelSubtasks
      val thisTask = getRuntimeContext.getIndexOfThisSubtask

      val allReadings = sensorsStateIt.asScala.toSeq
      this.readings = allReadings.zipWithIndex
        .filter(x => x._2 % numTasks == thisTask)
        .map(_._1)
        .toArray
    }
  }

  /** Save the current readings in the operator state. */
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    // replace sensor state by current readings
    this.sensorsState.update(readings.toList.asJava)
  }
}
