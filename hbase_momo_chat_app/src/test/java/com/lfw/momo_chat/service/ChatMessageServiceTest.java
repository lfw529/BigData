package com.lfw.momo_chat.service;

import com.lfw.momo_chat.entity.Msg;
import com.lfw.momo_chat.service.impl.HBaseNativeChatMessageService;
import com.lfw.momo_chat.service.impl.PhoenixChatMessageService;
import org.junit.Test;

import java.util.List;

public class ChatMessageServiceTest {
    private ChatMessageService chatMessageService;

    public ChatMessageServiceTest() throws Exception {
//        chatMessageService = new HBaseNativeChatMessageService();
        chatMessageService = new PhoenixChatMessageService();
    }

    //通过这个命令：hbase(main):031:0> scan "MOMO_CHAT:MSG", {LIMIT => 1, FORMATTER => "toString"}
    //来找出一对 sender_account 与 receiver_account, 时间 data: 为数据导入日期
    @Test
    public void getMessage() throws Exception {
        List<Msg> message = chatMessageService.getMessage("2022-03-25", "13857198987", "13641568674");
        for (Msg msg : message) {
            System.out.println(msg);
        }
    }
}
