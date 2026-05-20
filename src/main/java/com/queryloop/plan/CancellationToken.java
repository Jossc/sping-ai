package com.queryloop.plan;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 可传播的取消令牌，贯穿 Plan → Step → LLM 全链路。
 * 支持协作式中断：关键路径上调用 throwIfCancelled() 检查取消状态。
 */
public class CancellationToken {

    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final String planId;
    private final List<Runnable> cancelHooks = new CopyOnWriteArrayList<>();

    public CancellationToken(String planId) {
        this.planId = planId;
    }

    public boolean isCancelled() {
        return cancelled.get();
    }

    /**
     * 触发取消：设置标记 + 执行所有钩子（如中断 LLM 调用、关闭连接等）
     */
    public void cancel() {
        if (cancelled.compareAndSet(false, true)) {
            for (Runnable hook : cancelHooks) {
                try {
                    hook.run();
                } catch (Exception ignored) {
                }
            }
        }
    }

    /**
     * 注册取消钩子，用于中断阻塞操作（如 OkHttp call.cancel()）
     */
    public void onCancel(Runnable hook) {
        cancelHooks.add(hook);
    }

    /**
     * 在关键路径上检查取消状态，已取消则抛出 PlanCancelledException
     */
    public void throwIfCancelled() {
        if (cancelled.get()) {
            throw new PlanCancelledException(planId);
        }
    }
}
