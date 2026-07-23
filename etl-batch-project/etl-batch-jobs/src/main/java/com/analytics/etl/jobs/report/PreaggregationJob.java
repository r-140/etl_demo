package com.analytics.etl.jobs.report;

import com.analytics.etl.core.config.CustomerConfig;
import com.analytics.etl.core.metrics.ETLMetrics;
import com.analytics.etl.core.pipeline.ETLPipeline;
import com.analytics.etl.core.pipeline.PipelineDefinition;
import com.analytics.etl.core.pipeline.PipelineResult;
import com.analytics.etl.core.transform.AggregationTransform;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Pre-aggregation ETL Job.
 * 
 * Creates pre-computed aggregates for fast report queries:
 * - daily_sales_summary
 * - weekly_customer_ltv
 * - monthly_product_performance
 * - quarterly_vendor_performance
 */
public class PreaggregationJob {

    private static final Logger LOG = LoggerFactory.getLogger(PreaggregationJob.class);
    private static final String PIPELINE_ID = "preaggregation_pipeline";

    public static PipelineResult runDailySummary(SparkSession spark, CustomerConfig config, ETLMetrics metrics) {
        List transforms = List.of(
            new AggregationTransform("daily_sales",
                List.of("date_key"),
                List.of(
                    new AggregationTransform.Aggregation(AggregationTransform.Aggregation.Function.SUM, "total_amount", "revenue"),
                    new AggregationTransform.Aggregation(AggregationTransform.Aggregation.Function.COUNT, "order_id", "order_count"),
                    new AggregationTransform.Aggregation(AggregationTransform.Aggregation.Function.COUNT_DISTINCT, "customer_id", "unique_customers")
                ),
                List.of("1 day")
            )
        );

        PipelineDefinition definition = PipelineDefinition.builder()
            .name("daily_sales_summary")
            .sourceTable("fact_orders")
            .transforms(transforms)
            .targetPath("/data/warehouse/" + config.getCustomerId() + "/daily_sales_summary")
            .targetFormat("delta")
            .loadMode("overwrite")
            .build();

        ETLPipeline pipeline = new ETLPipeline(spark, PIPELINE_ID + "_daily", metrics, null, null);
        return pipeline.run(config, definition);
    }

    public static PipelineResult runWeeklyLTV(SparkSession spark, CustomerConfig config, ETLMetrics metrics) {
        List transforms = List.of(
            new AggregationTransform("weekly_ltv",
                List.of("customer_key", "week_key"),
                List.of(
                    new AggregationTransform.Aggregation(AggregationTransform.Aggregation.Function.SUM, "total_amount", "weekly_spend"),
                    new AggregationTransform.Aggregation(AggregationTransform.Aggregation.Function.COUNT, "order_id", "weekly_orders"),
                    new AggregationTransform.Aggregation(AggregationTransform.Aggregation.Function.AVG, "total_amount", "avg_order_value")
                ),
                List.of("7 days")
            )
        );

        PipelineDefinition definition = PipelineDefinition.builder()
            .name("weekly_customer_ltv")
            .sourceTable("fact_orders")
            .transforms(transforms)
            .targetPath("/data/warehouse/" + config.getCustomerId() + "/weekly_customer_ltv")
            .targetFormat("delta")
            .loadMode("overwrite")
            .build();

        ETLPipeline pipeline = new ETLPipeline(spark, PIPELINE_ID + "_weekly", metrics, null, null);
        return pipeline.run(config, definition);
    }
}
