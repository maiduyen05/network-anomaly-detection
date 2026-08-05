package com.network.preprocess;

import com.network.preprocess.bronze.BronzeJob;
import com.network.preprocess.gold.GoldJob;
import com.network.preprocess.silver.SilverJob;

import java.util.Arrays;
import java.util.Locale;

/**
 * Entry point mặc định của flink-preprocess fat JAR.
 *
 * <p>Cùng một JAR có thể khởi động từng tầng độc lập:</p>
 *
 * <pre>
 * java -jar flink-preprocess.jar bronze
 * java -jar flink-preprocess.jar silver
 * java -jar flink-preprocess.jar gold
 * </pre>
 *
 * <p>Ba tầng là ba Flink Job độc lập và giao tiếp qua Kafka.</p>
 */
public final class PreprocessJob {

    private PreprocessJob() {
    }

    public static void main(
            String[] args
    ) throws Exception {

        if (args == null || args.length == 0) {
            throw new IllegalArgumentException(
                    "Missing pipeline layer. "
                            + "Usage: PreprocessJob "
                            + "bronze|silver|gold"
            );
        }

        String layer =
                args[0]
                        .trim()
                        .toLowerCase(Locale.ROOT);

        /*
         * Bỏ đối số đầu tiên trước khi chuyển các đối số còn lại
         * cho entry point của tầng tương ứng.
         */
        String[] forwardedArgs =
                Arrays.copyOfRange(
                        args,
                        1,
                        args.length
                );

        switch (layer) {
            case "bronze" ->
                    BronzeJob.main(
                            forwardedArgs
                    );

            case "silver" ->
                    SilverJob.main(
                            forwardedArgs
                    );

            case "gold" ->
                    GoldJob.main(
                            forwardedArgs
                    );

            default ->
                    throw new IllegalArgumentException(
                            "Unsupported pipeline layer: "
                                    + args[0]
                                    + ". Expected bronze, silver or gold."
                    );
        }
    }
}