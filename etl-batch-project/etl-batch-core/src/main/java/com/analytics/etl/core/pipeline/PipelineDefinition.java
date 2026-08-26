package com.analytics.etl.core.pipeline;

import com.analytics.etl.core.error.QuarantineManager;
import com.analytics.etl.core.transform.SparkTransform;

import java.util.List;
import java.util.Map;

/**
 * Definition of an ETL pipeline: source, transforms, target, and configuration.
 */
public class PipelineDefinition {

    private String name;
    private String sourceTable;
    private Map<String, String> sourceOptions = Map.of();
    private List<SparkTransform> transforms = List.of();
    private List<QuarantineManager.ValidationRule> validationRules = List.of();
    private String targetPath;
    private String targetFormat = "delta";
    private String loadMode = "append"; // overwrite, append, merge
    private String mergeCondition;

    // Builder pattern
    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private PipelineDefinition def = new PipelineDefinition();

        public Builder name(String name) { def.name = name; return this; }
        public Builder sourceTable(String table) { def.sourceTable = table; return this; }
        public Builder sourceOptions(Map<String, String> opts) { def.sourceOptions = opts; return this; }
        public Builder transforms(List<SparkTransform> transforms) { def.transforms = transforms; return this; }
        public Builder validationRules(List<QuarantineManager.ValidationRule> rules) { def.validationRules = rules; return this; }
        public Builder targetPath(String path) { def.targetPath = path; return this; }
        public Builder targetFormat(String format) { def.targetFormat = format; return this; }
        public Builder loadMode(String mode) { def.loadMode = mode; return this; }
        public Builder mergeCondition(String condition) { def.mergeCondition = condition; return this; }

        public PipelineDefinition build() {
            if (def.name == null || def.name.isBlank()) throw new IllegalStateException("pipeline name is required");
            if (def.sourceTable == null || def.sourceTable.isBlank()) throw new IllegalStateException("source table is required");
            if (def.targetPath == null || def.targetPath.isBlank()) throw new IllegalStateException("target path is required");
            if ("merge".equals(def.loadMode) && (def.mergeCondition == null || def.mergeCondition.isBlank())) {
                throw new IllegalStateException("mergeCondition is required for merge mode");
            }
            return def;
        }
    }

    // Getters
    public String getName() { return name; }
    public String getSourceTable() { return sourceTable; }
    public Map<String, String> getSourceOptions() { return sourceOptions; }
    public List<SparkTransform> getTransforms() { return transforms; }
    public List<QuarantineManager.ValidationRule> getValidationRules() { return validationRules; }
    public String getTargetPath() { return targetPath; }
    public String getTargetFormat() { return targetFormat; }
    public String getLoadMode() { return loadMode; }
    public String getMergeCondition() { return mergeCondition; }
}
