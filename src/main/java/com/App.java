package com;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class App {
    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
        System.out.println("🚀 玄枢的 AI Demo 启动成功！访问: http://localhost:8080/ai/chat?msg=你好");
    }
}
