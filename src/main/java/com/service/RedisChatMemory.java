package com.service;

import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper; // 用于转 JSON
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RedisChatMemory implements ChatMemory {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public RedisChatMemory(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void add(String conversationId, List<Message> messages) {
        String key = "chat:" + conversationId;
        // 把消息转换成 JSON 字符串存入 Redis List
        for (Message msg : messages) {
            try {
                // 这里做一个简化的存储对象，只存 role 和 content
                // 实际生产中可能要存更多元数据
                String json = objectMapper.writeValueAsString(new SimpleMessage(msg.getMessageType().getValue(), msg.getContent()));
                redisTemplate.opsForList().rightPush(key, json);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // 设置过期时间，比如 7 天后遗忘 (防止内存爆了)
        redisTemplate.expire(key, java.time.Duration.ofDays(7));
    }

    @Override
    public List<Message> get(String conversationId, int lastN) {
        String key = "chat:" + conversationId;
        // 获取 Redis 里所有的聊天记录
        List<String> jsonList = redisTemplate.opsForList().range(key, 0, -1);

        if (jsonList == null) return new ArrayList<>();

        // 把 JSON 转回 Message 对象
        List<Message> allMessages = jsonList.stream().map(json -> {
            try {
                SimpleMessage sm = objectMapper.readValue(json, SimpleMessage.class);
                if ("user".equals(sm.role)) return new UserMessage(sm.content);
                if ("assistant".equals(sm.role)) return new AssistantMessage(sm.content);
                if ("system".equals(sm.role)) return new SystemMessage(sm.content);
                return null;
            } catch (Exception e) {
                return null;
            }
        }).filter(java.util.Objects::nonNull).collect(Collectors.toList());

        // 如果只需要最后 N 条 (lastN)，这里切片。但这步 Spring AI 好像会自己处理，我们先返回全部。
        return allMessages;
    }

    @Override
    public void clear(String conversationId) {
        redisTemplate.delete("chat:" + conversationId);
    }

    // 内部类，用来做 JSON 序列化
    record SimpleMessage(String role, String content) {}
}
