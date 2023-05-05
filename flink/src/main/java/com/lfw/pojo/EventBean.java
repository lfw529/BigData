package com.lfw.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventBean {
    private long guid;
    private String eventId;
    private long timeStamp;
    private String pageId;
    private int actTimelong;  // 行为时长
}