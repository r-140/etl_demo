package com.analytics.etl.core.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deduplication transform using window functions.
 * Keeps the latest record based on a timestamp column.
 */
public class DeduplicationTransform implements SparkTransform {

    private static final Logger LOG = LoggerFactory.getLogger(DeduplicationTransform.class);

    private final String[] keyColumns;
    private final String timestampColumn;
    private final String orderDirection;

    public DeduplicationTransform(String[] keyColumns, String timestampColumn) {
        this(keyColumns, timestampColumn, "desc");
    }

    public DeduplicationTransform(String[] keyColumns, String timestampColumn, String orderDirection) {
        this.keyColumns = keyColumns;
        this.timestampColumn = timestampColumn;
        this.orderDirection = orderDirection;
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input) {
        LOG.info("Deduplicating on keys: {} using timestamp: {}", 
                String.join(", ", keyColumns), timestampColumn);

        WindowSpec window = Window.partitionBy(keyColumns)
            .orderBy(functions.col(timestampColumn).desc());

        return input.withColumn("row_num", functions.row_number().over(window))
            .filter(functions.col("row_num").equalTo(1))
            .drop("row_num");
    }

    @Override
    public String getName() { return "deduplication"; }

    @Override
    public String getDescription() { 
        return "Deduplicate on " + String.join(", ", keyColumns); 
    }
}
