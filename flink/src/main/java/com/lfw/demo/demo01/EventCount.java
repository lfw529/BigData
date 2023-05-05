package com.lfw.demo.demo01;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventCount {
    private int id;
    private String eventId;
    private int cnt;
}
