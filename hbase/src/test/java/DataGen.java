import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class DataGen {
    public static void main(String[] args) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter("hbase/data/students.txt"));

        for (int i = 0; i < 100000; i++) {
            //000001,zhangsan,18,male,beijing,18000,2018-07-01

            //向 str 左填充某个字符串(padStr)，长度为 size 位
            String stu_id = StringUtils.leftPad(i + "", 6, "0");

            //生成一个长度为6的随机字母字符串
            String name = RandomStringUtils.randomAlphabetic(6);

            //随机在整数 18~38 之间生成年龄
            int age = RandomUtils.nextInt(18, 38);

            // 随机生成性别
            String gender = null;
            if (RandomUtils.nextInt() % 2 == 0) {
                gender = "male";
            } else {
                gender = "female";
            }

            //随机生成城市
            String city = null;
            int flag = RandomUtils.nextInt() % 3;
            if (flag == 0) {
                city = "北京";
            } else if (flag == 1) {
                city = "上海";
            } else {
                city = "南京";
            }

            int salary = RandomUtils.nextInt(12000, 32000);

            //随机生成格式："yyyy-MM-dd" 的日期，且年份在 2010-2020 之间
            String dt = RandomUtils.nextInt(2010, 2020) + "-" + StringUtils.leftPad(RandomUtils.nextInt(1, 13) + "", 2, "0") + "-" + StringUtils.leftPad(RandomUtils.nextInt(1, 31) + "", 2, "0");

            bw.write(stu_id + "," + name + "," + age + "," + gender + "," + city + "," + salary + "," + dt);
            bw.newLine();
        }
        bw.flush();
        bw.close();
    }
}
