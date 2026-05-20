package com.queryloop;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 工具注册中心 —— 管理所有可用工具的定义
 */
@Slf4j
@Component
public class ToolRegistry {

    private final Map<String, ToolDefinition> definitions = new LinkedHashMap<>();

    public ToolRegistry() {
        registerDefaults();
    }

    /** 注册默认工具（后续可通过外部配置扩展） */
    private void registerDefaults() {
        // 订单查询工具
        register(new ToolDefinition()
                .setName("orderQuery")
                .setDescription("查询护理订单状态和详情")
                .setBeanName("orderQueryFunction")
                .setPaginationSupported(true)
                .setFallbackMessage("订单系统暂时无法访问，请稍后再查。")
                .setRequiredParams(Map.of(
                        "orderId", new ToolDefinition.ParamDef()
                                .setName("orderId").setDescription("订单号").setType("STRING").setRequired(true)
                )));

        // 工单创建工具
        register(new ToolDefinition()
                .setName("workOrderCreate")
                .setDescription("创建新的护理工单")
                .setBeanName("workOrderCreateFunction")
                .setFallbackMessage("工单系统暂时无法访问，请稍后再试。")
                .setRequiredParams(Map.of(
                        "patientName", new ToolDefinition.ParamDef()
                                .setName("patientName").setDescription("老人姓名").setType("STRING").setRequired(true),
                        "serviceType", new ToolDefinition.ParamDef()
                                .setName("serviceType").setDescription("服务类型").setType("STRING").setRequired(true)
                )));

        // 物流追踪工具
        register(new ToolDefinition()
                .setName("logisticsTrace")
                .setDescription("追踪护理用品物流配送状态")
                .setBeanName("logisticsTraceFunction")
                .setPaginationSupported(true)
                .setFallbackMessage("物流系统暂时无法访问，请稍后再查。")
                .setRequiredParams(Map.of(
                        "trackingNumber", new ToolDefinition.ParamDef()
                                .setName("trackingNumber").setDescription("物流单号").setType("STRING").setRequired(true)
                )));

        log.info("[ToolRegistry] 已注册 {} 个工具: {}", definitions.size(), definitions.keySet());
    }

    public void register(ToolDefinition def) {
        definitions.put(def.getName(), def);
    }

    public Optional<ToolDefinition> get(String name) {
        return Optional.ofNullable(definitions.get(name));
    }

    public Collection<ToolDefinition> getAll() {
        return definitions.values();
    }
}
