package com.rag;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

// 这是一个数据载体，AI 会自动要把 location, reason, level 填进去
public record AlarmRequest(
        @JsonProperty(required = true)
        @JsonPropertyDescription("发生异常的位置，例如：302号房、食堂、走廊")
        String location,

        @JsonProperty(required = true)
        @JsonPropertyDescription("异常的具体原因，例如：老人摔倒、心率过高")
        String reason,

        @JsonProperty(required = true)
        @JsonPropertyDescription("紧急程度，只能是：P0(高危), P1(关注), P2(普通)")
        String level
) {}
