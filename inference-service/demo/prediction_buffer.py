from __future__ import annotations

import json
import threading

from collections import OrderedDict
from datetime import datetime, timezone

from confluent_kafka import Consumer


class PredictionBuffer:
    """
    Consumer nền dành riêng cho dashboard Streamlit.

    Luồng:

        anomaly-predictions
                ↓
        background Kafka thread
                ↓
        OrderedDict trong RAM
                ↓
        Streamlit snapshot()

    Tại sao không poll Kafka trực tiếp trong Streamlit?

    Vì Streamlit rerun code nhiều lần.
    Nếu mỗi rerun tạo một Consumer mới thì dễ gây:
    - Kafka group rebalance;
    - consumer bị tạo lặp;
    - state UI khó kiểm soát.

    Vì vậy Kafka consumer sống trong một background thread,
    còn Streamlit chỉ đọc snapshot đã được bảo vệ bằng Lock.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        max_records: int = 3000,
    ) -> None:

        self.bootstrap_servers = (
            bootstrap_servers
        )

        self.topic = topic

        self.group_id = group_id

        self.max_records = (
            max_records
        )


        # ====================================================
        # PREDICTION STORE
        # ====================================================
        #
        # key:
        #     prediction_id
        #
        # value:
        #     anomaly prediction JSON
        #
        # OrderedDict cho phép:
        # - giữ thứ tự arrival;
        # - deduplicate theo prediction_id;
        # - xóa record cũ nhất khi buffer đầy.
        # ====================================================

        self._records: OrderedDict[
            str,
            dict,
        ] = OrderedDict()


        # Kafka thread ghi.
        # Streamlit thread đọc.
        #
        # Vì vậy phải dùng Lock.
        self._lock = (
            threading.Lock()
        )


        self._stop_event = (
            threading.Event()
        )


        self._last_message_at: str | None = (
            None
        )

        self._last_error: str | None = (
            None
        )

        self._messages_received = 0


        # ====================================================
        # START BACKGROUND THREAD
        # ====================================================

        self._thread = (
            threading.Thread(
                target=
                    self._consume_loop,

                name=
                    "streamlit-prediction-consumer",

                daemon=True,
            )
        )


        self._thread.start()


    # ========================================================
    # INTERNAL CONSUMER LOOP
    # ========================================================

    def _consume_loop(
        self,
    ) -> None:
        """
        Consume prediction mới liên tục.

        Consumer group này chỉ dành cho dashboard.

        auto.offset.reset = latest
        --------------------------

        Khi dashboard start TRƯỚC runtime:

            prediction development / cũ
                      ↑
                    skip

            dashboard starts
                      ↓

            prediction runtime mới
                      ↓
                  hiển thị


        enable.auto.commit = True
        -------------------------

        Dashboard không phải critical processing service.

        Prediction mất khỏi dashboard khi UI crash không làm
        thay đổi inference correctness.

        Vì vậy dashboard có thể dùng auto commit đơn giản hơn.
        """
        consumer = Consumer(
            {
                "bootstrap.servers":
                    self.bootstrap_servers,

                "group.id":
                    self.group_id,

                # ====================================================
                # DASHBOARD DISPLAY POLICY
                # ====================================================
                #
                # Dashboard cần dựng lại toàn bộ prediction đã có
                # sau mỗi lần restart.
                #
                # Vì vậy đọc từ đầu topic anomaly-predictions.
                #
                # Topic này chỉ chứa OUTPUT của inference worker,
                # không phải Gold development data.
                # ====================================================
                "auto.offset.reset":
                    "earliest",

                # ====================================================
                # KHÔNG COMMIT OFFSET CHO DASHBOARD
                # ====================================================
                #
                # Dashboard chỉ là lớp hiển thị.
                #
                # Không commit giúp lần restart sau vẫn đọc lại toàn bộ
                # prediction trong Kafka và dựng lại RAM buffer.
                #
                # Inference worker vẫn commit Gold offset bình thường.
                # Hai việc hoàn toàn độc lập.
                # ====================================================
                "enable.auto.commit":
                    False,

                "client.id":
                    "streamlit-anomaly-dashboard",
            }
        )


        consumer.subscribe(
            [
                self.topic
            ]
        )


        try:

            while (
                not self
                ._stop_event
                .is_set()
            ):

                message = consumer.poll(
                    timeout=1.0
                )


                if message is None:
                    continue


                if message.error():

                    with self._lock:

                        self._last_error = (
                            str(
                                message.error()
                            )
                        )

                    continue


                try:

                    record = json.loads(
                        message
                        .value()
                        .decode(
                            "utf-8"
                        )
                    )


                    # ========================================
                    # DEDUPLICATION KEY
                    # ========================================

                    prediction_id = str(
                        record.get(
                            "prediction_id"
                        )
                        or
                        (
                            f"{message.partition()}"
                            f":"
                            f"{message.offset()}"
                        )
                    )


                    with self._lock:

                        # Nếu prediction này đã tồn tại
                        # do worker replay, bỏ bản cũ.
                        self._records.pop(
                            prediction_id,
                            None,
                        )


                        self._records[
                            prediction_id
                        ] = record


                        # Không để RAM tăng vô hạn.
                        while (
                            len(
                                self._records
                            )
                            >
                            self.max_records
                        ):

                            self._records.popitem(
                                last=False
                            )


                        self._messages_received += 1


                        self._last_message_at = (
                            datetime
                            .now(
                                timezone.utc
                            )
                            .isoformat()
                            .replace(
                                "+00:00",
                                "Z",
                            )
                        )


                        self._last_error = None


                except Exception as exc:

                    with self._lock:

                        self._last_error = (
                            repr(exc)
                        )


        finally:

            consumer.close()


    # ========================================================
    # PUBLIC API FOR STREAMLIT
    # ========================================================

    def snapshot(
        self,
    ) -> list[dict]:
        """
        Trả copy của prediction buffer.

        Không trả trực tiếp OrderedDict internal để tránh UI
        sửa nhầm state của Kafka consumer.
        """

        with self._lock:

            return list(
                self._records
                .values()
            )


    def status(
        self,
    ) -> dict:
        """
        Runtime status phục vụ dashboard.
        """

        with self._lock:

            return {
                "records_in_memory":
                    len(
                        self._records
                    ),

                "messages_received":
                    self._messages_received,

                "last_message_at":
                    self._last_message_at,

                "last_error":
                    self._last_error,

                "consumer_thread_alive":
                    self._thread.is_alive(),
            }


    def stop(
        self,
    ) -> None:
        """
        Cho phép stop thread nếu sau này cần cleanup rõ ràng.
        """

        self._stop_event.set()