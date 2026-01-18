package com.service;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.function.Function;

@Slf4j
@JsonClassDescription("股票查询请求")
@Service("mockStockService")
public class MockStockService implements Function<MockStockService.Request, MockStockService.Response> {

    @JsonClassDescription("包含股票代码")
    public record Request(@JsonProperty(required = true, value = "symbol") String symbol) {
    }

    public record Response(String symbol, double price) {
    }

    @Override
    public Response apply(Request request) {
        log.info("AI 正在调用我的 Java 方法查询股票:{}", request.symbol());
        // 模拟数据
        if ("AAPL".equalsIgnoreCase(request.symbol())) {
            return new Response("AAPL", 180.5);
        } else if ("GOOG".equalsIgnoreCase(request.symbol())) {
            return new Response("GOOG", 140.2);
        } else {
            return new Response(request.symbol(), 0.0);
        }
    }
}
