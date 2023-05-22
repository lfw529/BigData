package com.lfw.interceptor;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;

public class EncryptInterceptor implements Interceptor {
    //要加密的字段索引s
    String indices;

    //索引之间的分隔符
    String idxSplitBy;

    //数据体之间的分割符
    String dataSplitBy;

    /**
     * 构造方法
     */
    public EncryptInterceptor(String indices, String idxSplitBy, String dataSplitBy) {
        // 0,3
        this.indices = indices;
        this.idxSplitBy = idxSplitBy;
        this.dataSplitBy = dataSplitBy;
    }

    // 这个方法会被框架调用一次，用来做一些初始化工作
    @Override
    public void initialize() {

    }

    //拦截方法--对一个 event 进行处理
    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String dataStr = new String(body);

        //数据的字段数组
        String[] dataFieldsArr = dataStr.split(dataSplitBy);

        //需要加密的索引的数组
        String[] idxArr = indices.split(idxSplitBy);

        for (String s : idxArr) {  //0,3
            int index = Integer.parseInt(s);
            //取出要加密的字段内容
            String field = dataFieldsArr[index];
            //MD5 加密这个字段
            String encryptedField = DigestUtils.md5Hex(field);
            //BASE64 编码
            byte[] bytes = Base64.decodeBase64(encryptedField);
            //替换掉原来的未知加密内容
            dataFieldsArr[index] = new String(bytes);
        }

        //将加密过的字段重新拼接成一条数据，并使用原来的分割符
        StringBuilder sb = new StringBuilder();
        for (String field : dataFieldsArr) {
            sb.append(field).append(dataSplitBy);
        }

        sb.deleteCharAt(sb.lastIndexOf(dataSplitBy));

        //返回加密后的字段所封装的 event 对象
        return EventBuilder.withBody(sb.toString().getBytes());
    }

    //拦截方法———对一批 event 进行处理
    @Override
    public List<Event> intercept(List<Event> events) {
        ArrayList<Event> lst = new ArrayList<>();

        for (Event event : events) {
            Event eventEncrypt = intercept(event);
            lst.add(eventEncrypt);
        }
        return lst;
    }

    // agent退出前，会调一次该方法，进行需要的清理、关闭操作
    @Override
    public void close() {

    }

    //拦截器的构造器
    public static class EncryptInterceptorBuilder implements Interceptor.Builder {

        // 要加密的字段索引s
        String indices;
        // 索引之间的分隔符
        String idxSplitBy;
        // 数据体字段之间的分隔符
        String dataSplitBy;

        @Override
        public Interceptor build() {
            return new EncryptInterceptor(indices, idxSplitBy, dataSplitBy);
        }

        // 获取配置文件中的拦截器参数
        public void configure(Context context) {
            // 要加密的字段索引s
            this.indices = context.getString(Constants.INDICES);
            // 索引之间的分隔符
            this.idxSplitBy = context.getString(Constants.IDX_SPLIT_BY);
            // 数据体字段之间的分隔符
            this.dataSplitBy = context.getString(Constants.DATA_SPLIT_BY);
        }
    }

    public static class Constants {
        public static final String INDICES = "indices";
        public static final String IDX_SPLIT_BY = "idxSplitBy";
        public static final String DATA_SPLIT_BY = "dataSplitBy";
    }

}
