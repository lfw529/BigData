package com.lfw.tolerance.wal;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;

public class WriteAheadSink extends GenericWriteAheadSink<String> {

    public WriteAheadSink(CheckpointCommitter committer, TypeSerializer<String> serializer, String jobID) throws Exception {
        super(committer, serializer, jobID);
    }


    @Override
    protected boolean sendValues(Iterable<String> values, long checkpointId, long timestamp) throws Exception {
        for (String value : values) {
            if (value.equals("X") && RandomUtils.nextInt(1, 5) % 3 == 0)
                throw new Exception("error ......");
            System.out.println(checkpointId + "," + value);
        }
        return true;
    }
}
