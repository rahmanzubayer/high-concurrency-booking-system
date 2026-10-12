package com.zubayer.bookingingestservice;

import org.springframework.boot.SpringApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableScheduling
@SpringBootApplication
public class BookingIngestServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(BookingIngestServiceApplication.class, args);
    }

}
