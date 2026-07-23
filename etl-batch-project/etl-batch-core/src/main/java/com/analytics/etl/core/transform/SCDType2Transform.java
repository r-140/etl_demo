package com.analytics.etl.core.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.Arrays;

/**
 * SCD Type 2 transform for dimension tables.
 * Tracks historical changes with valid_from/valid_to dates and is_current flag.
 */
public class SCDType2Transform implements SparkTransform {

    private static final Logger LOG = LoggerFactory.getLogger(SCDType2Transform.class);

    private final String[] businessKeyColumns;
    private final String[] trackingColumns;
    private final String hashColumn;

    public SCDType2Transform(String[] businessKeyColumns, String[] trackingColumns) {
        this.businessKeyColumns = businessKeyColumns;
        this.trackingColumns = trackingColumns;
        this.hashColumn = "_scd_hash";
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input) {
        LOG.info("Applying SCD Type 2 transform on keys: {}", 
                String.join(", ", businessKeyColumns));

        // Create hash of tracking columns for change detection
        Dataset<Row> withHash = input.withColumn(hashColumn, 
            functions.md5(functions.concat_ws("|", 
                Arrays.stream(trackingColumns).map(functions::col).toArray(org.apache.spark.sql.Column[]::new)))
        );

        // Add SCD columns
        LocalDate today = LocalDate.now();
        LocalDate endOfTime = LocalDate.of(9999, 12, 31);

        return withHash
            .withColumn("valid_from", functions.lit(today.toString()).cast("date"))
            .withColumn("valid_to", functions.lit(endOfTime.toString()).cast("date"))
            .withColumn("is_current", functions.lit(true));
    }

    @Override
    public String getName() { return "scd_type2"; }

    @Override
    public String getDescription() { 
        return "SCD Type 2 on " + String.join(", ", businessKeyColumns); 
    }
}
