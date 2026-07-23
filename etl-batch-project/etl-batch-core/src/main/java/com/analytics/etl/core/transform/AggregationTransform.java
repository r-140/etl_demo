package com.analytics.etl.core.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Aggregation transform with support for multiple aggregation levels.
 * Typical use case: pre-aggregating data for reports (daily, weekly, monthly).
 */
public class AggregationTransform implements SparkTransform {

    private static final Logger LOG = LoggerFactory.getLogger(AggregationTransform.class);

    private final String name;
    private final List<String> groupByColumns;
    private final List<Aggregation> aggregations;
    private final List<String> timeWindows; // "1 day", "7 days", "30 days"

    public AggregationTransform(String name, List<String> groupByColumns, 
                                List<Aggregation> aggregations, List<String> timeWindows) {
        this.name = name;
        this.groupByColumns = groupByColumns;
        this.aggregations = aggregations;
        this.timeWindows = timeWindows;
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input) {
        LOG.info("Applying aggregation transform: {} with groupBy: {}", name, groupByColumns);

        Dataset<Row> result = input;

        for (String window : timeWindows) {
            // Add time window column
            result = result.withColumn("time_window", 
                functions.window(functions.col("event_date"), window));

            // Build aggregation expressions
            for (Aggregation agg : aggregations) {
                switch (agg.getFunction()) {
                    case SUM -> result = result.groupBy(groupByColumns.toArray(new String[0]))
                        .agg(functions.sum(agg.getColumn()).alias(agg.getAlias()));
                    case COUNT -> result = result.groupBy(groupByColumns.toArray(new String[0]))
                        .agg(functions.count(agg.getColumn()).alias(agg.getAlias()));
                    case AVG -> result = result.groupBy(groupByColumns.toArray(new String[0]))
                        .agg(functions.avg(agg.getColumn()).alias(agg.getAlias()));
                    case MAX -> result = result.groupBy(groupByColumns.toArray(new String[0]))
                        .agg(functions.max(agg.getColumn()).alias(agg.getAlias()));
                    case MIN -> result = result.groupBy(groupByColumns.toArray(new String[0]))
                        .agg(functions.min(agg.getColumn()).alias(agg.getAlias()));
                    case COUNT_DISTINCT -> result = result.groupBy(groupByColumns.toArray(new String[0]))
                        .agg(functions.countDistinct(agg.getColumn()).alias(agg.getAlias()));
                }
            }
        }

        return result;
    }

    @Override
    public String getName() { return name; }

    @Override
    public String getDescription() { 
        return "Aggregation on " + String.join(", ", groupByColumns); 
    }

    public static class Aggregation {
        public enum Function { SUM, COUNT, AVG, MAX, MIN, COUNT_DISTINCT }

        private final Function function;
        private final String column;
        private final String alias;

        public Aggregation(Function function, String column, String alias) {
            this.function = function;
            this.column = column;
            this.alias = alias;
        }

        public Function getFunction() { return function; }
        public String getColumn() { return column; }
        public String getAlias() { return alias; }
    }
}
