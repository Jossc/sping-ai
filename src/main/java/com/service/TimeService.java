package com.service;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;

@Slf4j
@JsonClassDescription("获取当前系统的准确日期和时间")
public class TimeService implements Function<TimeService.Request, TimeService.Response> {

    @JsonClassDescription
    public record Request(String timeZone){

    }

    public record Response(String dateTime, String weekDay) {

    }


    @Override
    public TimeService.Response apply(TimeService.Request request) {
        // 4. 获取真正的当前时间！
        LocalDateTime now = LocalDateTime.now();

        // 格式化一下，让 AI 读得更舒服
        String timeStr = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String weekStr = now.getDayOfWeek().toString(); // 比如 MONDAY

        System.out.println("⏰ AI 正在调用我的 TimeService，它想知道时间！");

        return new Response(timeStr, weekStr);
    }


}
