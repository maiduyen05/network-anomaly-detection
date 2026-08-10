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
 * Entry point của log-producer.
 *
 * <p>
 * Pipeline:
 * </p>
 *
 * <pre>
 * input directory
 *       ↓
 * FileLogReader
 *       ↓
 * RawNetworkEventFactory (chuyển về định dạng JSON)
 *       ↓
 * RawNetworkEventJsonSerializer
 *       ↓
 * KafkaRawEventPublisher  (khởi tạo publisher đẩy DL vào Kafka)
 *       ↓
 * raw.ue.log.line
 * </pre>
 *
 * <p>
 * Producer chỉ chịu trách nhiệm ingest raw log.
 * Nó không parse 52 field nghiệp vụ.
 * Việc parse/validate/transform thuộc Bronze Flink Job.
 * </p>
 */
public final class LogProducerApplication {

    /**
     * Logger của application.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(
                    LogProducerApplication.class
            );


    /**
     * Utility/application class không cần instance.
     */
    private LogProducerApplication() {
    }


    /**
     * Entry point.
     *
     * <p>
     * Có hai cách chạy:
     * </p>
     *
     * <pre>
     * 1 argument:
     *
     * LogProducerApplication input-directory
     *
     * -> dùng application.properties trong classpath.
     *
     *
     * 2 arguments:
     *
     * LogProducerApplication input-directory config-file
     *
     * -> dùng configuration file bên ngoài.
     * </pre>
     */
    public static void main(
            String[] args
    ) {

        /*
         * =========================================================
         * VALIDATE COMMAND LINE
         * =========================================================
         */

        if (args.length < 1
                || args.length > 2) {

            System.err.println(
                    "Usage: LogProducerApplication "
                            + "<input-directory> [config-file]"
            );

            System.exit(
                    1
            );
        }


        /*
         * =========================================================
         * INPUT DIRECTORY
         * =========================================================
         */

        Path inputDirectory =
                Path.of(
                        args[0]
                );


        try {

            /*
             * =====================================================
             * LOAD CONFIGURATION
             * =====================================================
             *
             * 1 argument:
             *
             *     classpath application.properties
             *
             * 2 arguments:
             *
             *     external properties file
             */

            ProducerConfiguration configuration =
                    args.length == 2
                            ? ProducerConfiguration.load(
                                    Path.of(
                                            args[1]
                                    )
                            )
                            : ProducerConfiguration.loadDefault();


            /*
             * =====================================================
             * EXECUTE PRODUCER PIPELINE
             * =====================================================
             */

            run(
                    inputDirectory,
                    configuration
            );

        } catch (Exception exception) {

            LOGGER.error(
                    "Producer execution failed",
                    exception
            );

            System.exit(
                    1
            );
        }
    }


    /**
     * Thực thi pipeline:
     *
     * <pre>
     * files
     *   ↓
     * raw envelope
     *   ↓
     * Kafka
     * </pre>
     */
    private static void run(
            Path inputDirectory,
            ProducerConfiguration configuration
    ) throws IOException {

        /*
         * =========================================================
         * BƯỚC 1
         * VALIDATE INPUT DIRECTORY
         * =========================================================
         */

        validateInputDirectory(
                inputDirectory
        );


        /*
         * =========================================================
         * BƯỚC 2
         * LIST INPUT FILES
         * =========================================================
         */

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


        /*
         * =========================================================
         * BƯỚC 3
         * KHỞI TẠO CÁC THÀNH PHẦN PIPELINE
         * =========================================================
         */

        FileLogReader fileLogReader =
                new FileLogReader();

        RawNetworkEventFactory eventFactory =
                new RawNetworkEventFactory();

        RawNetworkEventJsonSerializer serializer =
                new RawNetworkEventJsonSerializer();

        KafkaMessageKeyResolver keyResolver =
                new KafkaMessageKeyResolver();


        /*
         * =========================================================
         * BƯỚC 4
         * COUNTERS
         * =========================================================
         */

        AtomicLong queuedCount =
                new AtomicLong();

        AtomicLong successCount =
                new AtomicLong();

        AtomicLong failureCount =
                new AtomicLong();


        /*
         * =========================================================
         * BƯỚC 5
         * CREATE KAFKA PRODUCER + PUBLISHER
         * =========================================================
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

            /*
             * =====================================================
             * BƯỚC 6
             * ĐỌC TỪNG FILE
             * =====================================================
             */

            for (Path inputFile
                    : inputFiles) {

                LOGGER.info(
                        "Reading input file: {}",
                        inputFile.getFileName()
                );


                /*
                 * FileLogReader đọc từng dòng.
                 *
                 * Với mỗi dòng:
                 *
                 * SourceLine
                 *      ↓
                 * RawNetworkEvent
                 *      ↓
                 * Kafka
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

                                                /*
                                                 * Kafka xác nhận thành công.
                                                 */
                                                if (exception == null) {

                                                    successCount
                                                            .incrementAndGet();

                                                    return;
                                                }


                                                /*
                                                 * Kafka send lỗi.
                                                 */
                                                failureCount
                                                        .incrementAndGet();


                                                /*
                                                 * Không log raw payload
                                                 * vì raw log có thể chứa
                                                 * dữ liệu thuê bao.
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
             * =====================================================
             * BƯỚC 7
             * FLUSH
             * =====================================================
             *
             * Chờ toàn bộ message bất đồng bộ
             * được Kafka trả kết quả.
             */

            publisher.flush();
        }


        /*
         * =========================================================
         * BƯỚC 8
         * VERIFY SEND RESULT
         * =========================================================
         */

        LOGGER.info(
                "Producer completed: "
                        + "queued={}, "
                        + "success={}, "
                        + "failed={}",
                queuedCount.get(),
                successCount.get(),
                failureCount.get()
        );


        /*
         * Có ít nhất một record gửi lỗi.
         */
        if (failureCount.get() > 0) {

            throw new IllegalStateException(
                    "Some messages failed to send: "
                            + failureCount.get()
            );
        }


        /*
         * Mọi record queued phải có callback success.
         */
        if (successCount.get()
                != queuedCount.get()) {

            throw new IllegalStateException(
                    "Message count mismatch: queued="
                            + queuedCount.get()
                            + ", success="
                            + successCount.get()
            );
        }
    }


    /**
     * Kiểm tra input directory tồn tại.
     */
    private static void validateInputDirectory(
            Path inputDirectory
    ) {

        if (!Files.isDirectory(
                inputDirectory
        )) {

            throw new IllegalArgumentException(
                    "Input directory does not exist: "
                            + inputDirectory
            );
        }
    }


    /**
     * Lấy tất cả regular file trong input directory
     * và sort theo tên.
     *
     * <p>
     * Sort giúp cùng một directory luôn được đọc
     * theo thứ tự deterministic.
     * </p>
     */
    private static List<Path> listInputFiles(
            Path inputDirectory
    ) throws IOException {

        try (
                Stream<Path> paths =
                        Files.list(
                                inputDirectory
                        )
        ) {

            return paths
                    .filter(
                            Files::isRegularFile
                    )
                    .sorted()
                    .toList();
        }
    }
}