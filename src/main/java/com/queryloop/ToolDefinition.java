package com.queryloop;

import lombok.Data;
import lombok.experimental.Accessors;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 工具元数据定义
 */
@Data
@Accessors(chain = true)
public class ToolDefinition {

    /** 工具唯一名称（对应 Spring AI Function Bean 名） */
    private String name;

    /** 工具中文描述（用于 LLM Function Calling） */
    private String description;

    /** 触发该工具的意图类型 */
    private IntentType triggerIntent = IntentType.TOOL_CALL;

    /** 调用超时 */
    private Duration timeout = Duration.ofSeconds(10);

    /** 降级兜底回复（超时或异常时返回给用户的消息） */
    private String fallbackMessage = "服务暂时不可用，请稍后再试。";

    /** 必选参数定义 */
    private Map<String, ParamDef> requiredParams = new LinkedHashMap<>();

    /** 可选参数定义 */
    private Map<String, ParamDef> optionalParams = new LinkedHashMap<>();

    /** 工具对应的 Spring Bean 名称 */
    private String beanName;

    /** 是否支持分页（结果可被后续追问复用） */
    private boolean paginationSupported = false;

    @Data
    @Accessors(chain = true)
    public static class ParamDef {
        private String name;
        private String description;
        private String type; // STRING, NUMBER, BOOLEAN
        private boolean required = false;
    }
}
