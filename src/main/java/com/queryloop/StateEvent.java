package com.queryloop;

import lombok.Getter;

import java.time.LocalDateTime;

/**
 * 状态总线上的变更事件。
 *
 * <p>每次 {@link StateBus#publish} 将产生一个 StateEvent，
 * 包含变更前后的完整 SessionState 快照，保证订阅者读到一致的状态。</p>
 */
@Getter
public class StateEvent {

    /** 用户 ID */
    private final String userId;

    /** 变更前的状态（首次写入时为 null） */
    private final SessionState previous;

    /** 变更后的状态（始终非 null） */
    private final SessionState current;

    /** 变更来源（如 "StateWriter", "MasterAgent", "FeedbackController"） */
    private final String source;

    /** 事件序号（同一用户内单调递增） */
    private final long sequence;

    /** 事件时间 */
    private final LocalDateTime timestamp;

    public StateEvent(String userId, SessionState previous, SessionState current,
                      String source, long sequence) {
        this.userId = userId;
        this.previous = previous;
        this.current = current;
        this.source = source;
        this.sequence = sequence;
        this.timestamp = LocalDateTime.now();
    }

    // ── 便捷判断 ──

    /** 是否为首个事件（用户首次创建会话） */
    public boolean isFirstEvent() {
        return previous == null;
    }

    /** 与上一状态比较：意图是否发生变化 */
    public boolean intentChanged() {
        return previous != null && previous.getLastIntent() != current.getLastIntent();
    }

    /** 与上一状态比较：激活上下文是否发生变化 */
    public boolean contextChanged() {
        return previous != null && previous.getActiveContext() != current.getActiveContext();
    }

    // ── Getters ──

    @Override
    public String toString() {
        return String.format("StateEvent{user=%s seq=%d intent=%s ctx=%s src=%s}",
                userId, sequence,
                current.getLastIntent(), current.getActiveContext(), source);
    }
}
