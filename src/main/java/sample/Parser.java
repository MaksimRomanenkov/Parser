package sample;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class Parser {
    public static final String ERROR = "ERROR";
    public static final DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd, HH:mm");
    public static final DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("-HH:mm Количество ошибок: ");

    public static final ConcurrentMap<LocalDateTime, Integer> RESULT = new ConcurrentHashMap<>();
    public static ExecutorService executorService;

    public static void main(String[] args) throws IOException {
        long startRead = System.currentTimeMillis();

        Path inputDir = Paths.get("input");
        int length = Objects.requireNonNull(inputDir.toFile().list()).length;
        executorService = Executors.newFixedThreadPool(length);

        readFiles(inputDir);
        shutdown();
        System.out.println("Time read: " + (System.currentTimeMillis() - startRead) + " ms");

        long startWrite = System.currentTimeMillis();
        writeToFile();
        System.out.println("Time write: " + (System.currentTimeMillis() - startWrite) + " ms");
    }

    private static void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException ex) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static void readFiles(Path inputDir) throws IOException {
        try (Stream<Path> files = Files.walk(inputDir)) {
            files.filter(Files::isRegularFile)
                    .forEach(path -> executorService.execute(() -> {
                        System.out.println("start " + path + " " + Thread.currentThread().getName());
                        try {
                            readFromFile(path);
                        } catch (IOException e) {
                            System.out.println(e);
                        }
                        System.out.println("end " + path);
                    }));
        }
    }

    private static void readFromFile(Path path) throws IOException {
        Files.lines(path)
                .parallel()
                .filter(s -> s.contains(ERROR))
                .forEach(s -> {
                    String time = s.substring(0, s.indexOf(';'));
                    LocalDateTime key = LocalDateTime.parse(time).truncatedTo(ChronoUnit.HOURS);
                    RESULT.merge(key, 1, Integer::sum);
                });
    }

    private static void writeToFile() throws IOException {
        File dir = new File("output");
        if (!dir.exists()) dir.mkdirs();
        Files.write(
                Path.of("output/result.txt"),
                () -> Parser.RESULT.keySet().stream()
                        .sorted()
                        .<CharSequence>map(key -> key.format(formatter1) + key.plusHours(1).format(formatter2) + Parser.RESULT.get(key))
                        .iterator()
        );
    }
}
