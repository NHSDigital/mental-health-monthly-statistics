-- Databricks notebook source
CREATE TABLE IF NOT EXISTS $db_output.FYFV_unformatted
(
    REPORTING_PERIOD_START DATE,
    REPORTING_PERIOD_END DATE,
    STATUS STRING,
    BREAKDOWN string,
    PRIMARY_LEVEL string,
    PRIMARY_LEVEL_DESCRIPTION string,
    SECONDARY_LEVEL string,
    SECONDARY_LEVEL_DESCRIPTION string,
    METRIC string,
    METRIC_VALUE float,
    SOURCE_DB string
)
USING DELTA
PARTITIONED BY (REPORTING_PERIOD_END, STATUS)