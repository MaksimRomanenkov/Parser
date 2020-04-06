package sample;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.stream.IntStream;

public class Generate {
    public static void main(String[] args) {
        generateFiles(Integer.MAX_VALUE / 1000, 5, "input");
    }

    private static void generateFiles(int records, int files, String folder) {
        long now = Instant.now().toEpochMilli();

        File dir = new File(folder);
        if (!dir.exists()) dir.mkdirs();

        IntStream.range(0, files)
                .parallel()
                .forEach(fileCount -> {
                    try {
                        try (BufferedWriter writer = new BufferedWriter(new FileWriter(folder + "/logfile" + fileCount + ".txt", true))) {
                            for (long i = records; i >= 0; i--) {
                                String error = Math.random() > 0.5 ? "ERROR" : "WARN";
                                long time = Math.round(Math.random() * now);
                                if (time % 1000 == 0) {
                                    time += 777;
                                }
                                writer.write(LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneOffset.UTC).toString() + ";" + error + ";message" + System.lineSeparator());
                            }
                        }
                    } catch (Exception ex) {
                        System.out.println(ex);
                    }
                });
    }
}
