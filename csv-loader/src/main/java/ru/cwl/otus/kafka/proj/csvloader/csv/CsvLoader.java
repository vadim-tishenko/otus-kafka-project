package ru.cwl.otus.kafka.proj.csvloader.csv;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

@Slf4j
public class CsvLoader {

    @Value(value = "${pathtocsvfile}")
     String pathToFile;
    private static final String COMMA_DELIMITER = ",";
    String fileName = "/home/vad/dev/learning/kafka/otus-kafka-project/tmp/1.csv";


    public String ptcf(){
        return pathToFile;
    }

    public void load() {
        log.info("start");
        List<String[]> strings = Collections.emptyList();
        Path p = Path.of(fileName);
        try {
            readAllLines3(p);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        log.info("finish s:{}", strings.size());
    }

    public List<String[]> readAllLines(Path filePath) throws Exception {
        try (Reader reader = Files.newBufferedReader(filePath)) {
            try (CSVReader csvReader = new CSVReader(reader)) {


                return csvReader.readAll();

            }
        }
    }

    public void readAllLines3(Path filePath) throws Exception {
        int count = 0;
        try (Reader reader = Files.newBufferedReader(filePath)) {
            try (CSVReader csvReader = new CSVReader(reader)) {

                while (true) {
                    String[] nextLineAsTokens = csvReader.readNext();
                    if (nextLineAsTokens == null) break;
                    count++;
                    //allElements.add(nextLineAsTokens);
                }


                //return csvReader.readAll();

            }
        }
        log.info("reads {} lines", count);
    }

    public void readAllLines3(Path filePath, Consumer<String[]> consumer)  {
        int count = 0;
        try (Reader reader = Files.newBufferedReader(filePath);
             CSVReader csvReader = new CSVReader(reader)) {
            csvReader.skip(1);
            while (true) {
                String[] nextLineAsTokens = csvReader.readNext();
                if (nextLineAsTokens == null) break;
                consumer.accept(nextLineAsTokens);
                count++;
            }
        } catch (IOException | CsvValidationException e) {
            throw new RuntimeException(e);
        }
        log.info("reads {} lines", count);
    }

    public List<String[]> readAllLines2() {
        int count = 0;
        List<String[]> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(COMMA_DELIMITER);
                count++;
//                records.add(values);
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.info("count {}", count);
        return records;
    }
}