package com.logicbig.example;

public class Running1Consumers {
    public static void main(String[] args) throws Exception {
        ConsumerGroupExample.run(1, new String[]{"test-consumer-group"});
    }
}