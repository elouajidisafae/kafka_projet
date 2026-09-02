"""
Tests du calcul du lag et de la classification des alertes.

C'est le coeur pédagogique du projet :
    lag = log_end_offset - committed_offset

On teste :
- La formule de base
- Les cas limites (lag négatif, partitions multiples)
- La classification OK / WARNING / CRITICAL
"""
import pytest
from unittest.mock import patch, MagicMock
from dataclasses import dataclass

from core.lag_calculator import (
    PartitionLag,
    ConsumerGroupStatus,
    _compute_status,
    compute_lag_for_group,
)


class TestComputeStatus:
    """Tests de la classification des alertes."""

    def test_ok_when_lag_below_warning(self):
        """Lag en dessous du seuil warning → statut OK."""
        with patch("core.lag_calculator.CONFIG", {
            "alerts": {"warning_threshold": 1000, "critical_threshold": 10000}
        }):
            assert _compute_status(0)   == "OK"
            assert _compute_status(500) == "OK"
            assert _compute_status(999) == "OK"

    def test_warning_when_lag_at_threshold(self):
        """Lag exactement au seuil warning → WARNING."""
        with patch("core.lag_calculator.CONFIG", {
            "alerts": {"warning_threshold": 1000, "critical_threshold": 10000}
        }):
            assert _compute_status(1000) == "WARNING"
            assert _compute_status(5000) == "WARNING"
            assert _compute_status(9999) == "WARNING"

    def test_critical_when_lag_at_threshold(self):
        """Lag exactement au seuil critical → CRITICAL."""
        with patch("core.lag_calculator.CONFIG", {
            "alerts": {"warning_threshold": 1000, "critical_threshold": 10000}
        }):
            assert _compute_status(10000) == "CRITICAL"
            assert _compute_status(50000) == "CRITICAL"

    def test_zero_lag_is_ok(self):
        """Lag = 0 signifie consumer à jour → OK."""
        with patch("core.lag_calculator.CONFIG", {
            "alerts": {"warning_threshold": 1000, "critical_threshold": 10000}
        }):
            assert _compute_status(0) == "OK"

    def test_custom_thresholds(self):
        """Les seuils personnalisés sont bien pris en compte."""
        with patch("core.lag_calculator.CONFIG", {
            "alerts": {"warning_threshold": 100, "critical_threshold": 500}
        }):
            assert _compute_status(99)  == "OK"
            assert _compute_status(100) == "WARNING"
            assert _compute_status(500) == "CRITICAL"


class TestPartitionLag:
    """Tests du calcul du lag par partition."""

    def test_lag_formula(self):
        """
        Formule fondamentale : lag = log_end_offset - committed_offset

        EXEMPLE :
            LEO = 5000 (dernier message produit)
            committed = 4800 (dernier message traité)
            lag = 5000 - 4800 = 200
        """
        p = PartitionLag(
            partition_id=0,
            log_end_offset=5000,
            committed_offset=4800,
            lag=200,
        )
        assert p.lag == p.log_end_offset - p.committed_offset

    def test_zero_lag_when_caught_up(self):
        """Consumer à jour → lag = 0."""
        p = PartitionLag(
            partition_id=0,
            log_end_offset=3000,
            committed_offset=3000,
            lag=0,
        )
        assert p.lag == 0

    def test_multiple_partitions_total_lag(self):
        """
        Le lag total = somme des lags de toutes les partitions.

        EXEMPLE :
            Partition 0 : lag = 200
            Partition 1 : lag = 300
            Partition 2 : lag = 0
            Total = 500
        """
        partitions = [
            PartitionLag(0, 5000, 4800, 200),
            PartitionLag(1, 6000, 5700, 300),
            PartitionLag(2, 4000, 4000, 0),
        ]
        total_lag = sum(p.lag for p in partitions)
        assert total_lag == 500


class TestConsumerGroupStatus:
    """Tests du statut global d'un consumer group."""

    def test_total_lag_computed_from_partitions(self):
        """Le total_lag est calculé automatiquement depuis les partitions."""
        with patch("core.lag_calculator.CONFIG", {
            "alerts": {"warning_threshold": 1000, "critical_threshold": 10000}
        }):
            status = ConsumerGroupStatus(
                cluster_name="dev",
                group_id="my-group",
                topic="orders",
                partitions=[
                    PartitionLag(0, 5000, 4500, 500),
                    PartitionLag(1, 3000, 2500, 500),
                ]
            )
        assert status.total_lag == 1000
        assert status.status == "WARNING"

    def test_empty_partitions_lag_zero(self):
        """Sans partitions, le lag total est 0."""
        with patch("core.lag_calculator.CONFIG", {
            "alerts": {"warning_threshold": 1000, "critical_threshold": 10000}
        }):
            status = ConsumerGroupStatus(
                cluster_name="dev",
                group_id="my-group",
                topic="orders",
                partitions=[]
            )
        assert status.total_lag == 0
        assert status.status == "OK"

    def test_status_critical_with_high_lag(self):
        """Lag total > seuil critique → statut CRITICAL."""
        with patch("core.lag_calculator.CONFIG", {
            "alerts": {"warning_threshold": 1000, "critical_threshold": 10000}
        }):
            status = ConsumerGroupStatus(
                cluster_name="prod",
                group_id="slow-group",
                topic="orders",
                partitions=[
                    PartitionLag(0, 20000, 5000, 15000),
                ]
            )
        assert status.status == "CRITICAL"
        assert status.total_lag == 15000


class TestInvalidCommittedOffsets:
    def test_invalid_offset_is_excluded_from_lag(self):
        class Probe:
            def list_topics(self, topic, timeout):
                class Metadata:
                    pass
                metadata = Metadata()
                metadata.topics = {topic: Metadata()}
                metadata.topics[topic].partitions = {0: None, 1: None, 2: None}
                return metadata

            def get_watermark_offsets(self, topic_partition, timeout):
                return (0, {0: 100, 1: 200, 2: 300}[topic_partition.partition])

            def close(self):
                pass

        class CommittedConsumer:
            def committed(self, topic_partitions, timeout):
                from confluent_kafka import TopicPartition
                return [
                    TopicPartition("orders", 0, 90),
                    TopicPartition("orders", 1, -1001),
                    TopicPartition("orders", 2, 250),
                ]

            def close(self):
                pass

        with patch("core.lag_calculator._make_consumer", return_value=Probe()), \
             patch("core.lag_calculator.Consumer", return_value=CommittedConsumer()):
            result = compute_lag_for_group("dev", "kafka", "group", "orders")

        assert result.partition_count == 3
        assert result.partitions_counted == 2
        assert [p.partition_id for p in result.partitions] == [0, 2]
        assert result.total_lag == (100 - 90) + (300 - 250)