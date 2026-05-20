package com.queryloop;

/**
 * 用户意图分类枚举
 */
public enum IntentType {

    /** 需要调用外部工具 (股票查询、时间获取、报警触发等) */
    TOOL_CALL,

    /** 需要检索知识库 (向量搜索 RAG) */
    RAG_SEARCH,

    /** 询问历史对话中提及的内容 */
    MEMORY_QUERY,

    /** 普通闲聊，直接对话即可 */
    GENERAL_CHAT
}
