package com.network.producer;

import com.network.producer.config.ProducerConfiguration;
import com.network.producer.factory.RawNetworkEventFactory;
import com.network.producer.kafka.KafkaMessageKeyResolver;
import com.network.producer.kafka.KafkaProducerFactory;
import com.network.producer.kafka.KafkaRawEventPublisher;
import com.network.producer.model.RawNetworkEvent;
import com.network.producer.reader.FileLogReader;
import com.network.producer.serialization.RawNetworkEventJsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Kết nối toàn bộ các file log-producer
 * Thư mục ban đầu --> Lấy danh sách file
 * --> FileLogReader(đọc từng dòng)
 * --> RawNetworkEventFactory (chuyển về định dạng JSON)
 * --> KafkaRawEventPublisher (khởi tạo publisher đẩy DL vào kafka)
 * --> Kafka
 */
public final class LogProducerApplication {

    /**
     * Logger dùng thay cho System.out.println().
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(
                    LogProducerApplication.class
            );

    /**
     * Đường dẫn cấu hình mặc định khi không truyền tham số thứ hai.
     */
    private static final Path DEFAULT_CONFIG_PATH =
            Path.of(
                    "log-producer",
                    "src",
                    "main",
                    "resources",
                    "application.properties"
            );

    /**
     * Constructor private vì application được chạy qua main().
     */
    private LogProducerApplication() {
    }

    /**
     * Chạy ứng dụng.
     *
     * <p>Cú pháp:</p>
     *
     * <pre>
     * LogProducerApplication input-directory [config-file]
     * </pre>
     */
    public static void main(String[] args) {

        if (args.length < 1 || args.length > 2) {
            System.err.println(
                    "Usage: LogProducerApplication "
                            + "<input-directory> [config-file]"
            );

            System.exit(1);
        }

        Path inputDirectory =
                Path.of(args[0]);

        Path configPath =
                args.length == 2
                        ? Path.of(args[1])
                        : DEFAULT_CONFIG_PATH;

        try {
            run(
                    inputDirectory,
                    configPath
            );

        } catch (Exception exception) {
            LOGGER.error(
                    "Producer execution failed",
                    exception
            );

            System.exit(1);
        }
    }

    /**
     * Thực thi pipeline file → Kafka.
     */
    private static void run(
            Path inputDirectory,
            ProducerConfiguration configuration
    ) throws IOException {

        validateInputDirectory(
                inputDirectory
        );

        // Đọc cấu hình application và Kafka.
        ProducerConfiguration configuration =
        args.length == 2
                ? ProducerConfiguration.load(
                        Path.of(args[1])
                )
                : ProducerConfiguration.loadDefault();

        run(
                inputDirectory,
                configuration
        );

        // Liệt kê file theo thứ tự tên để kết quả dễ tái lập.
        List<Path> inputFiles =
                listInputFiles(
                        inputDirectory
                );

        if (inputFiles.isEmpty()) {
            throw new IllegalArgumentException(
                    "No input files found in: "
                            + inputDirectory
            );
        }

        // Các thành phần của pipeline.
        FileLogReader fileLogReader =
                new FileLogReader();

        RawNetworkEventFactory eventFactory =
                new RawNetworkEventFactory();

        RawNetworkEventJsonSerializer serializer =
                new RawNetworkEventJsonSerializer();

        KafkaMessageKeyResolver keyResolver =
                new KafkaMessageKeyResolver();

        // Bộ đếm theo dõi kết quả.
        AtomicLong queuedCount =
                new AtomicLong();

        AtomicLong successCount =
                new AtomicLong();

        AtomicLong failureCount =
                new AtomicLong();

        /*
         * Tạo KafkaProducer thật.
         *
         * try-with-resources bảo đảm producer được đóng
         * khi hoàn thành hoặc khi có lỗi.
         */
        try (
                KafkaProducer<String, String> kafkaProducer =
                        KafkaProducerFactory.create(
                                configuration
                        );

                KafkaRawEventPublisher publisher =
                        new KafkaRawEventPublisher(
                                kafkaProducer,
                                configuration.topic(),
                                serializer,
                                keyResolver
                        )
        ) {
            for (Path inputFile : inputFiles) {

                LOGGER.info(
                        "Reading input file: {}",
                        inputFile.getFileName()
                );

                /*
                 * FileLogReader đọc từng dòng.
                 *
                 * Với mỗi SourceLine:
                 * 1. Tạo RawNetworkEvent.
                 * 2. Gửi event bất đồng bộ vào Kafka.
                 */
                long fileLineCount =
                        fileLogReader.read(
                                inputFile,
                                sourceLine -> {

                                    RawNetworkEvent event =
                                            eventFactory.create(
                                                    sourceLine
                                            );

                                    queuedCount.incrementAndGet();

                                    publisher.publish(
                                            event,
                                            (
                                                    metadata,
                                                    exception
                                            ) -> {
                                                if (
                                                        exception
                                                                == null
                                                ) {
                                                    successCount
                                                            .incrementAndGet();

                                                } else {
                                                    failureCount
                                                            .incrementAndGet();

                                                    /*
                                                     * Không log rawPayload vì
                                                     * chứa dữ liệu thuê bao.
                                                     */
                                                    LOGGER.error(
                                                            "Kafka send failed: "
                                                                    + "file={}, "
                                                                    + "line={}",
                                                            sourceLine
                                                                    .sourceFile(),
                                                            sourceLine
                                                                    .lineNumber(),
                                                            exception
                                                    );
                                                }
                                            }
                                    );
                                }
                        );

                LOGGER.info(
                        "Queued file: {}, lines={}",
                        inputFile.getFileName(),
                        fileLineCount
                );
            }

            /*
             * Chờ toàn bộ message trong buffer được gửi xong.
             *
             * Sau flush, successCount + failureCount
             * phải bằng queuedCount.
             */
            publisher.flush();
        }

        LOGGER.info(
                "Producer completed: "
                        + "queued={}, "
                        + "success={}, "
                        + "failed={}",
                queuedCount.get(),
                successCount.get(),
                failureCount.get()
        );

        if (failureCount.get() > 0) {
            throw new IllegalStateException(
                    "Some messages failed to send: "
                            + failureCount.get()
            );
        }

        if (successCount.get() != queuedCount.get()) {
            throw new IllegalStateException(
                    "Message count mismatch: queued="
                            + queuedCount.get()
                            + ", success="
                            + successCount.get()
            );
        }
    }

    /**
     * Kiểm tra thư mục input.
     */
    private static void validateInputDirectory(
            Path inputDirectory
    ) {
        if (!Files.isDirectory(inputDirectory)) {
            throw new IllegalArgumentException(
                    "Input directory does not exist: "
                            + inputDirectory
            );
        }
    }

    /**
     * Lấy danh sách file thường và sắp xếp theo tên.
     */
    private static List<Path> listInputFiles(
            Path inputDirectory
    ) throws IOException {

        try (
                Stream<Path> paths =
                        Files.list(inputDirectory)
        ) {
            return paths
                    .filter(Files::isRegularFile)
                    .sorted()
                    .toList();
        }
    }
}