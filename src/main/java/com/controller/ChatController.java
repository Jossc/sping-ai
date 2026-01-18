package com.controller;


import com.rag.RagService;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;

import lombok.val;
import org.apache.tomcat.util.json.JSONParser;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.openai.OpenAiChatOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@Slf4j
@RestController
public class ChatController {

    @Resource
    private RagService ragService;

    private final ChatModel chatModel;

    // 构造器注入 ChatModel (Spring AI 自动配置好的)
    @Autowired
    public ChatController(ChatModel chatModel) {
        this.chatModel = chatModel;
    }

    @GetMapping("/ai/chat")
    public String chat(@RequestParam(value = "msg", defaultValue = "给我讲个笑话") String msg) {
        OpenAiChatOptions openAiChatOptions
                = OpenAiChatOptions.builder()
                .withFunction("mockStockService").build();

        Prompt prompt = new Prompt(msg, openAiChatOptions);
        return chatModel.call(prompt).getResult().getOutput().getContent();

    }

    @GetMapping("/ai/agent")
    public String agentChat(@RequestParam(value = "msg") String msg) {

        OpenAiChatOptions options = OpenAiChatOptions.builder()
                .withFunctions(java.util.Set.of("stockFunction", "timeFunction"))
                .build();

        Prompt prompt = new Prompt(msg, options);
        return chatModel.call(prompt).getResult().getOutput().getContent();
    }


  /*  @GetMapping("/ai/rag")
    public String ragChat(
            @RequestParam(value = "msg") String msg,
            @RequestParam(value = "uid", defaultValue = "default-user") String uid) {
        return ragService.chatWithRag(msg, uid);
    }*/

    @GetMapping(value = "/ai/rag", produces = "text/event-stream;charset=UTF-8")
    public Flux<String> ragChat(
            @RequestParam(value = "msg") String msg,
            @RequestParam(value = "uid", defaultValue = "default-user") String uid
    ) {
        Flux<String> stringFlux = ragService.chatWithRagWithFlux(msg, uid);
        log.info("ragChat rest {} ", stringFlux);
        return stringFlux;
    }
}
