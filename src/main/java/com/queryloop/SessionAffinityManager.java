package com.queryloop;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 会话亲和性管理器 — Phase 3 分布式改造
 *
 * 策略：基于 userId 的一致性哈希，将同一会话连续请求路由到同一实例。
 * 仅在实例故障时自动转移。
 *
 * 当前为单实例实现，多实例时接入 Redis Pub/Sub 或服务注册中心。
 */
@Slf4j
@Component
public class SessionAffinityManager {

    /** 当前实例 ID（启动时从环境变量或随机生成） */
    private final String instanceId;

    /** 会话 → 实例 映射（生产环境移入 Redis） */
    private final ConcurrentMap<String, String> sessionInstanceMap = new ConcurrentHashMap<>();

    /** 可用实例列表（生产环境从注册中心获取） */
    private final ConcurrentMap<String, InstanceInfo> instances = new ConcurrentHashMap<>();

    public SessionAffinityManager() {
        this.instanceId = generateInstanceId();
        // 注册当前实例
        instances.put(instanceId, new InstanceInfo(instanceId, true));
        log.info("[SessionAffinity] 当前实例 {} 已注册，可用实例数: {}", instanceId, instances.size());
    }

    /**
     * 为会话分配实例
     *
     * @param sessionId 会话 ID (userId)
     * @return 目标实例 ID
     */
    public String assignInstance(String sessionId) {
        // 已有绑定 → 检查目标实例是否存活
        String existing = sessionInstanceMap.get(sessionId);
        if (existing != null && isInstanceAlive(existing)) {
            return existing;
        }

        // 新绑定 → 一致性哈希选择实例（当前单实例直接返回自己）
        String target = selectByConsistentHash(sessionId);
        sessionInstanceMap.put(sessionId, target);
        log.info("[SessionAffinity] session={} → instance={}", sessionId, target);
        return target;
    }

    /**
     * 解除会话绑定（会话结束时调用）
     */
    public void releaseSession(String sessionId) {
        sessionInstanceMap.remove(sessionId);
        log.debug("[SessionAffinity] 释放会话 {}", sessionId);
    }

    /**
     * 标记实例为不可用（故障转移触发）
     */
    public void markInstanceDown(String instanceId) {
        InstanceInfo info = instances.get(instanceId);
        if (info != null) {
            info.setAlive(false);
            // 清除该实例的所有会话绑定
            sessionInstanceMap.entrySet().removeIf(e -> instanceId.equals(e.getValue()));
            log.warn("[SessionAffinity] 实例 {} 已标记为不可用，{} 个会话将被重新分配",
                    instanceId, sessionInstanceMap.size());
        }
    }

    // ═══ 内部方法 ═══

    private String selectByConsistentHash(String key) {
        // 单实例直接返回当前实例
        // 多实例时：对所有 alive 实例做 MD5(key + instanceId) 排序取最近的
        long aliveCount = instances.values().stream().filter(InstanceInfo::isAlive).count();
        if (aliveCount <= 1) return instanceId;

        String selected = instanceId;
        long minHash = Long.MAX_VALUE;
        for (InstanceInfo info : instances.values()) {
            if (!info.isAlive()) continue;
            long hash = hash(key + ":" + info.getInstanceId());
            if (hash < minHash) {
                minHash = hash;
                selected = info.getInstanceId();
            }
        }
        return selected;
    }

    private boolean isInstanceAlive(String instanceId) {
        InstanceInfo info = instances.get(instanceId);
        return info != null && info.isAlive();
    }

    private long hash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(input.getBytes());
            return ((long) (digest[7] & 0xFF) << 56)
                 | ((long) (digest[6] & 0xFF) << 48)
                 | ((long) (digest[5] & 0xFF) << 40)
                 | ((long) (digest[4] & 0xFF) << 32)
                 | ((long) (digest[3] & 0xFF) << 24)
                 | ((long) (digest[2] & 0xFF) << 16)
                 | ((long) (digest[1] & 0xFF) << 8)
                 | ((long) (digest[0] & 0xFF));
        } catch (NoSuchAlgorithmException e) {
            return input.hashCode();
        }
    }

    private String generateInstanceId() {
        String envId = System.getenv("INSTANCE_ID");
        if (envId != null && !envId.isBlank()) return envId;
        return "node-" + Long.toHexString(System.currentTimeMillis() % 0xFFFF);
    }

    public String getInstanceId() {
        return instanceId;
    }

    // ═══ 内部类 ═══
    @lombok.Data
    @lombok.AllArgsConstructor
    static class InstanceInfo {
        private String instanceId;
        private boolean alive;
    }
}
