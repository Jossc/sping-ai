package com.queryloop;

/**
 * 用户对意图分类结果的反馈动作
 */
public enum IntentAdoption {
    /** 用户采纳了分类结果（使用了对应功能并得到了满意回答） */
    ADOPTED,

    /** 用户纠正了分类结果（分类错误，用户手动指定了正确的意图） */
    CORRECTED,

    /** 用户忽略了分类结果（没有继续该对话或切换了话题） */
    IGNORED
}
