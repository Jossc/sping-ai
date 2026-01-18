package com.config;

import com.rag.AlarmRequest;
import com.service.MockStockService;
import com.service.TimeService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Description;
import java.util.function.Function;

@Configuration
public class ToolsConfig {

    @Bean
    @Description("根据股票代码查询当前价格")
    public Function<MockStockService.Request, MockStockService.Response> stockFunction() {
        return new MockStockService();
    }

    @Bean
    @Description("获取当前的准确时间、日期和星期几")
    public Function<TimeService.Request, TimeService.Response> timeFunction() {
        return new TimeService();
    }
    @Bean
    @Description("当发生老人摔倒、异常报警等紧急情况时，调用此工具进行报警")
    public Function<AlarmRequest, String> alarmService() {
        return request -> {
            // 1. 这里写你真实的业务逻辑 (存数据库、发飞书、发短信)
            // 我们先用红色日志模拟一下
            System.err.println("🚨🚨🚨 [真实报警触发] 🚨🚨🚨");
            System.err.println("📍 位置: " + request.location());
            System.err.println("⚠️ 原因: " + request.reason());
            System.err.println("🔥 级别: " + request.level());
            System.err.println("📡 正在呼叫项目经理 & 120救护车...");

            // 2. 返回给 AI 的执行结果
            // AI 收到这个字符串后，会再把它组织成自然语言告诉用户
            return "报警成功！已通知项目经理，并在后台记录了 P0 级异常。位置：" + request.location();
        };
    }
}