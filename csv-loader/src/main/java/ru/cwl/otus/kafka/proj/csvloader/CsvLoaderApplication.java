package ru.cwl.otus.kafka.proj.csvloader;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import ru.cwl.otus.kafka.proj.csvloader.csv.CsvLoader;
import ru.cwl.otus.kafka.proj.csvloader.kafka.KafkaGo;

import java.nio.file.Path;

@Slf4j
@SpringBootApplication
public class CsvLoaderApplication {

    @Value(value = "${pathtocsvfile}")
    static String pathToFile;

    public static void main(String[] args) {
        final ConfigurableApplicationContext ctx = SpringApplication.run(CsvLoaderApplication.class, args);
        final CsvLoader csvLoader = new CsvLoader();
        final KafkaGo kg = ctx.getBean(KafkaGo.class);
        final AppConfig config = ctx.getBean(AppConfig.class);

		final Path path = Path.of(config.getPath());

        log.info("config: {}",config);
        log.info("start");
        csvLoader.readAllLines3(path, kg::consumer);
        log.info("finish");

    }


}
