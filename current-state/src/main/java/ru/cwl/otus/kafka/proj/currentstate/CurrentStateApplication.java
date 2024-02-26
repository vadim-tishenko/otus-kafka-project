package ru.cwl.otus.kafka.proj.currentstate;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class CurrentStateApplication {

    public static void main(String[] args) {
        SpringApplication.run(CurrentStateApplication.class, args);
        log.info("STARTED");
    }

}
