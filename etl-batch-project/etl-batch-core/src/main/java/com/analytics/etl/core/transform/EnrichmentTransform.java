package com.analytics.etl.core.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Enrichment transform: joins input with reference data (lookup tables).
 * Supports broadcast joins for small lookup tables.
 */
public class EnrichmentTransform implements SparkTransform {

    private static final Logger LOG = LoggerFactory.getLogger(EnrichmentTransform.class);

    private final String name;
    private final String lookupTable;
    private final String joinKey;
    private final String[] selectColumns;
    private final boolean broadcastJoin;
    private final SparkSession spark;

    public EnrichmentTransform(SparkSession spark, String name, String lookupTable,
                               String joinKey, String[] selectColumns, boolean broadcastJoin) {
        this.spark = spark;
        this.name = name;
        this.lookupTable = lookupTable;
        this.joinKey = joinKey;
        this.selectColumns = selectColumns;
        this.broadcastJoin = broadcastJoin;
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input) {
        LOG.info("Enriching with lookup table: {} on key: {}", lookupTable, joinKey);

        Dataset<Row> lookupDF = spark.read().table(lookupTable).select(selectColumns);

        if (broadcastJoin) {
            lookupDF = org.apache.spark.sql.functions.broadcast(lookupDF);
        }

        return input.join(lookupDF, input.col(joinKey).equalTo(lookupDF.col(joinKey)), "left")
            .drop(lookupDF.col(joinKey));
    }

    @Override
    public String getName() { return name; }

    @Override
    public String getDescription() { 
        return "Enrich with " + lookupTable; 
    }
}
