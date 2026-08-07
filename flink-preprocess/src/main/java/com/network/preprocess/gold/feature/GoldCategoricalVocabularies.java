package com.network.preprocess.gold.feature;

import com.network.preprocess.config.GoldFeatureContract;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Factory chuyển categorical feature trong GoldFeatureContract
 * thành CategoricalVocabulary dùng bởi GoldFeatureEncoder.
 * Mapping category -> ID đến từ application.yaml:
 */
public final class GoldCategoricalVocabularies {

    private GoldCategoricalVocabularies() {
        // Utility class không cần tạo object.
    }

    /**
     * Tạo vocabulary từ một categorical feature
     * đã được parse từ feature-contract.
     *
     * @param feature categorical feature trong contract
     * @return vocabulary dùng để encode dữ liệu runtime
     */
    public static CategoricalVocabulary fromFeature(
            GoldFeatureContract.CategoricalFeature feature
    ) {

        Objects.requireNonNull(
                feature,
                "feature must not be null"
        );

        /*
         * GoldFeatureContract lưu vocabulary:
         *
         * Map<String, Integer>
         *
         * trong khi CategoricalVocabulary hiện sử dụng:
         *
         * Map<String, Long>
         *
         * Vì tensor categorical cuối cùng là long[][].
         */
        Map<String, Long> mapping =
                new LinkedHashMap<>();

        for (
                Map.Entry<String, Integer> entry
                        : feature.vocabulary().entrySet()
        ) {

            mapping.put(
                    entry.getKey(),
                    entry.getValue().longValue()
            );
        }

        return new CategoricalVocabulary(
                feature.name(),
                mapping
        );
    }
}