package com.network.preprocess.silver;

import com.network.preprocess.model.SilverEvent;
import com.network.preprocess.silver.dedup.SilverDedupKeySelector;
import com.network.preprocess.testsupport.SilverEventFixtures;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class SilverDedupKeySelectorTest {

    private final SilverDedupKeySelector selector =
            new SilverDedupKeySelector();

    @Test
    void shouldCreateSameKeyForSameKafkaSourceOffset()
            throws Exception {

        SilverEvent first =
                SilverEventFixtures.event(
                        100L,
                        "2026-07-08T10:15:30Z"
                );

        SilverEvent replayed =
                SilverEventFixtures.event(
                        100L,
                        "2026-07-08T10:15:35Z"
                );

        /*
         * Hai object Java khác nhau nhưng cùng source offset
         * phải nhận cùng dedup key.
         */
        assertEquals(
                selector.getKey(first),
                selector.getKey(replayed)
        );
    }

    @Test
    void shouldCreateDifferentKeyForDifferentOffsets()
            throws Exception {

        SilverEvent first =
                SilverEventFixtures.event(
                        100L,
                        "2026-07-08T10:15:30Z"
                );

        SilverEvent second =
                SilverEventFixtures.event(
                        101L,
                        "2026-07-08T10:15:31Z"
                );

        assertNotEquals(
                selector.getKey(first),
                selector.getKey(second)
        );
    }
}