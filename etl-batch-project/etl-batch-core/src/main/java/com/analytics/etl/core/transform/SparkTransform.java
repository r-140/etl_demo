package com.analytics.etl.core.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Base interface for Spark transformations.
 * Each transform takes a Dataset and returns a transformed Dataset.
 */
public interface SparkTransform {

    Dataset<Row> transform(Dataset<Row> input);
    String getName();
    String getDescription();
}
