package com.network.preprocess;

import com.network.preprocess.bronze.BronzeJob;
import com.network.preprocess.silver.SilverJob;

import java.util.Arrays;
import java.util.Locale;

/**
 * Entry point mặc định của flink-preprocess fat JAR.
 *
 * <p>Cho phép cùng một JAR khởi động từng tầng độc lập:</p>
 *
 * <pre>
 * java -jar flink-preprocess.jar bronze
 * java -jar flink-preprocess.jar silver
 * </pre>
 *
 * <p>Trong Flink production vẫn có thể dùng -c để chỉ định trực tiếp
 * BronzeJob hoặc SilverJob.</p>
 */
public final class PreprocessJob {

    private PreprocessJob() {
    }

    public static void main(String[] args)
            throws Exception {

        if (args == null || args.length == 0) {
            throw new IllegalArgumentException(
                    "Missing pipeline layer. "
                            + "Usage: PreprocessJob bronze|silver"
            );
        }

        String layer =
                args[0]
                        .trim()
                        .toLowerCase(Locale.ROOT);

        /*
         * Bỏ đối số đầu tiên trước khi chuyển phần còn lại
         * cho entry point của từng job.
         */
        String[] forwardedArgs =
                Arrays.copyOfRange(
                        args,
                        1,
                        args.length
                );

        switch (layer) {
            case "bronze" ->
                    BronzeJob.main(forwardedArgs);

            case "silver" ->
                    SilverJob.main(forwardedArgs);

            default ->
                    throw new IllegalArgumentException(
                            "Unsupported pipeline layer: "
                                    + args[0]
                                    + ". Expected bronze or silver."
                    );
        }
    }
}