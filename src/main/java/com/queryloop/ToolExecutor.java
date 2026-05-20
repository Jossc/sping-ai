package com.queryloop;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * 工具执行器 —— 带超时、降级、结果缓存
 *
 * 后续可接入真实的业务 RPC/HTTP 调用。
 * 当前提供模拟实现用于验证链路。
 */
@Slf4j
@Component
public class ToolExecutor {

    private final ToolRegistry registry;

    /** 用于超时控制的线程池 */
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public ToolExecutor(ToolRegistry registry) {
        this.registry = registry;
    }

    /**
     * 执行工具调用
     *
     * @param toolName 工具名
     * @param params   调用参数
     * @return 工具执行结果
     */
    public ToolResult execute(String toolName, Map<String, Object> params) {
        ToolDefinition def = registry.get(toolName)
                .orElseThrow(() -> new IllegalArgumentException("未知工具: " + toolName));

        long t0 = System.currentTimeMillis();
        Future<ToolResult> future = executor.submit(() -> executeInternal(toolName, params, def));

        try {
            return future.get(def.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.warn("[ToolExecutor] 工具 {} 超时 ({}ms)，返回降级回复", toolName, def.getTimeout().toMillis());
            future.cancel(true);
            return ToolResult.fallback(toolName, def.getFallbackMessage(),
                    System.currentTimeMillis() - t0);
        } catch (Exception e) {
            log.error("[ToolExecutor] 工具 {} 执行异常 err={}", toolName, e.toString());
            return ToolResult.fallback(toolName, def.getFallbackMessage(),
                    System.currentTimeMillis() - t0);
        }
    }

    /**
     * 实际执行逻辑（可替换为真实 RPC/HTTP 调用）
     */
    private ToolResult executeInternal(String toolName, Map<String, Object> params, ToolDefinition def) {
        long t0 = System.currentTimeMillis();

        return switch (toolName) {
            case "orderQuery"    -> executeOrderQuery(params, t0);
            case "workOrderCreate" -> executeWorkOrderCreate(params, t0);
            case "logisticsTrace"  -> executeLogisticsTrace(params, t0);
            default -> ToolResult.fail(toolName, "工具未实现", System.currentTimeMillis() - t0);
        };
    }

    // ── 工具实现（模拟，后续替换为真实业务调用） ──

    private ToolResult executeOrderQuery(Map<String, Object> params, long t0) {
        String orderId = (String) params.getOrDefault("orderId", "unknown");
        // 模拟 200ms 业务延迟
        sleep(200);

        ToolResult result = ToolResult.success("orderQuery",
                "订单 " + orderId + " 状态：进行中，护工已接单，预计14:30到达", System.currentTimeMillis() - t0);
        result.getData().put("orderId", orderId);
        result.getData().put("status", "进行中");
        result.getData().put("caregiver", "张护工");
        result.getData().put("estimatedArrival", "14:30");
        result.setPagination(new ToolResult.Pagination().setCurrentPage(1).setHasMore(false));
        return result;
    }

    private ToolResult executeWorkOrderCreate(Map<String, Object> params, long t0) {
        String patientName = (String) params.getOrDefault("patientName", "unknown");
        String serviceType = (String) params.getOrDefault("serviceType", "日常护理");
        sleep(300);

        String workOrderId = "WO-" + System.currentTimeMillis() % 100000;
        ToolResult result = ToolResult.success("workOrderCreate",
                "工单 " + workOrderId + " 已创建：为" + patientName + "提供" + serviceType + "服务",
                System.currentTimeMillis() - t0);
        result.getData().put("workOrderId", workOrderId);
        result.getData().put("patientName", patientName);
        result.getData().put("serviceType", serviceType);
        result.getData().put("status", "待接单");
        return result;
    }

    private ToolResult executeLogisticsTrace(Map<String, Object> params, long t0) {
        String trackingNo = (String) params.getOrDefault("trackingNumber", "unknown");
        sleep(150);

        ToolResult result = ToolResult.success("logisticsTrace",
                "物流单号 " + trackingNo + "：已到达本地分拣中心，预计明日送达",
                System.currentTimeMillis() - t0);
        result.getData().put("trackingNumber", trackingNo);
        result.getData().put("status", "运输中");
        result.getData().put("currentLocation", "本地分拣中心");
        result.getData().put("estimatedDelivery", "2026-05-17");
        result.setPagination(new ToolResult.Pagination().setCurrentPage(1).setHasMore(false));
        return result;
    }

    private void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
    }
}
