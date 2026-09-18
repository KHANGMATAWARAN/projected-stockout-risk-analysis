# Databricks notebook source
## 1. Load stockout risk dataset
df =  spark.table("stockout_risk_analysis")
display(df)

# COMMAND ----------

## 2. Validate Baseline Stockout Risk
stockout_df = df.filter(df.Stockout_Risk_Flag == 1)
stockout_materials = stockout_df.select("Material_ID").distinct().count()
print(stockout_materials)

# COMMAND ----------

## 3. Identify First Projected Stockout Month

first_stockout = (stockout_df
.groupBy("Material_ID")
.agg({"Month": "min"})
.withColumnRenamed("min(Month)", "First_Stockout_Month")
)

display(first_stockout)

# COMMAND ----------

## 4. Analyze Stockout Risk Timing

stockout_timing = (first_stockout
.groupBy("First_Stockout_Month")
.count()
.orderBy("First_Stockout_Month")
)

display(stockout_timing)

# COMMAND ----------

## 5. Compare Supply Scenarios

from pyspark.sql import functions as F
from pyspark.sql.window import Window

window_spec = (
    Window
    .partitionBy("Material_ID")
    .orderBy("Month")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

scenario_df = df.withColumn(
    "Projected_Inventory_With_Planned",
    F.col("Starting_Inventory")
    + F.sum(
        F.col("Confirmed_Supply")
        + F.col("Planned_Supply")
        - F.col("Forecast_Units")
    ).over(window_spec)
)

display(scenario_df)


# COMMAND ----------

## 6. Quantify Potentially Mitigated Stockout Risks

planned_scenario_risk = (
scenario_df
.filter(F.col("Projected_Inventory_With_Planned") < 0)
.select("Material_ID")
.distinct()
.count()
)

print("Confirmed Supply Only:", stockout_materials)
print("Confirmed + Planned Supply:", planned_scenario_risk)
print("Potential Risks Mitigated:", stockout_materials - planned_scenario_risk)

# COMMAND ----------

## 7. Prioritize Residual Stockout Risks

residual_risk = (
scenario_df
.filter(F.col("Projected_Inventory_With_Planned") < 0)
.groupBy("Material_ID", "Site_ID")
.agg(F.min("Projected_Inventory_With_Planned").alias("Lowest_Projected_Inventory"))
.orderBy("Lowest_Projected_Inventory")
)

display(residual_risk)

# COMMAND ----------

## 8. Evaluate At-Risk Supply scenario

scenario_3_df = df.withColumn(
    "Projected_Inventory_With_At_Risk",
    F.col("Starting_Inventory")
    + F.sum(
        F.col("Confirmed_Supply")
        + F.col("Planned_Supply")
        + F.col("At_Risk_Supply")
        - F.col("Forecast_Units")
    ).over(window_spec)
)

scenario_3_risk = (
    scenario_3_df
    .filter(F.col("Projected_Inventory_With_At_Risk") < 0)
    .select("Material_ID")
    .distinct()
    .count()
)

print("Confirmed Supply Only:", stockout_materials)
print("Confirmed + Planned Supply:", planned_scenario_risk)
print("Confirmed + Planned + At-Risk Supply:", scenario_3_risk)
print(
    "Additional Risks Potentially Mitigated by At-Risk Supply:",
    planned_scenario_risk - scenario_3_risk
)

# COMMAND ----------

## 9. Create Material-Level Risk Prioritization

# Summarize the 139 materials that remain in stockout
# after Confirmed + Planned supply is considered
risk_summary = (
    scenario_df
    .filter(F.col("Projected_Inventory_With_Planned") < 0)
    .groupBy("Material_ID", "Site_ID")
    .agg(
        F.min("Month").alias("First_Stockout_Month"),
        F.min("Projected_Inventory_With_Planned").alias(
            "Worst_Projected_Inventory"
        )
    )
)

# Identify materials that remain in stockout
# even after At-Risk supply is included
critical_materials = (
    scenario_3_df
    .filter(F.col("Projected_Inventory_With_At_Risk") < 0)
    .select("Material_ID")
    .distinct()
    .withColumn("Critical_Stockout_Flag", F.lit(1))
)

# Assign final stockout status
priority_df = (
    risk_summary
    .join(critical_materials, "Material_ID", "left")
    .fillna({"Critical_Stockout_Flag": 0})
    .withColumn(
        "Stockout_Status",
        F.when(
            F.col("Critical_Stockout_Flag") == 1,
            "Critical Stockout"
        ).otherwise("Stockout")
    )
    .orderBy("Worst_Projected_Inventory")
)

display(priority_df)

# COMMAND ----------

## 10. Prepare Stockout Severity Metrics

final_risk_df = (
    priority_df
    .withColumn(
        "Projected_Shortfall_Units",
        F.abs(F.col("Worst_Projected_Inventory"))
    )
    .select(
        "Material_ID",
        "Site_ID",
        "Stockout_Status",
        "First_Stockout_Month",
        "Projected_Shortfall_Units"
    )
    .orderBy(F.desc("Projected_Shortfall_Units"))
)

display(final_risk_df)

# COMMAND ----------

## 11. Add Material Attributes

material_master_df = spark.table("material_master_raw")

print(material_master_df.columns)

# COMMAND ----------

## 11. Add Material Attributes to Stockout Analysis

final_risk_enriched_df = (
    final_risk_df
    .join(
        material_master_df.select(
            "Material_ID",
            "Material_Description",
            "GPL_Code",
            "GPL_Name",
            "Primary_Site",
            "Site_Name",
            "Site_Region",
            "Application_Segment",
            "Lifecycle",
            "Lead_Time_Days",
            "Unit_Price_USD"
        ),
        "Material_ID",
        "left"
    )
)

display(final_risk_enriched_df)

# COMMAND ----------

## 12. Quantify Projected Shortfall Value

final_stockout_df = (
    final_risk_enriched_df
    .withColumnRenamed(
        "Projected_Shortfall_Units",
        "Stockout_Units"
    )
    .withColumn(
        "Stockout_Value_USD",
        F.round(
            F.col("Stockout_Units") * F.col("Unit_Price_USD"),
            2
        )
    )
    .orderBy(F.desc("Stockout_Value_USD"))
)

display(final_stockout_df)

# COMMAND ----------

## 13. Validate and Save Final Stockout Dataset

print("Total Stockout Materials:", final_stockout_df.count())

final_stockout_df.groupBy("Stockout_Status").count().show()

# COMMAND ----------

## 14. Check if there's any NULLs

final_stockout_df.select(
    [
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in [
            "Material_ID",
            "Site_ID",
            "Stockout_Status",
            "First_Stockout_Month",
            "Stockout_Units",
            "Unit_Price_USD",
            "Stockout_Value_USD"
        ]
    ]
).show()

# COMMAND ----------

final_stockout_df.write.mode("overwrite").saveAsTable(
    "stockout_risk_powerbi"
)

# COMMAND ----------

spark.table("stockout_risk_powerbi").count()

# COMMAND ----------

## 14. Driver Investigation

# Reload final stockout dataset
from pyspark.sql import functions as F

final_stockout_df = spark.table("stockout_risk_powerbi")

print("Final Stockout Materials:", final_stockout_df.count())

# 14.1 Lead Time Analysis
lead_time_analysis = (
    final_stockout_df
    .withColumn(
        "Lead_Time_Band",
        F.when(F.col("Lead_Time_Days") <= 30, "≤30 Days")
         .when(F.col("Lead_Time_Days") <= 60, "31–60 Days")
         .when(F.col("Lead_Time_Days") <= 90, "61–90 Days")
         .otherwise(">90 Days")
    )
    .groupBy("Lead_Time_Band", "Stockout_Status")
    .agg(
        F.countDistinct("Material_ID").alias("Material_Count"),
        F.round(F.sum("Stockout_Value_USD"), 2).alias("Stockout_Value_USD")
    )
    .orderBy("Lead_Time_Band", "Stockout_Status")
)

display(lead_time_analysis)




# COMMAND ----------

# 14.2 Critical Stockout Rate by Lead Time

material_master_df = spark.table("material_master_raw")

material_lead_time = (
    material_master_df
    .withColumn(
        "Lead_Time_Band",
        F.when(F.col("Lead_Time_Days") <= 30, "≤30 Days")
         .when(F.col("Lead_Time_Days") <= 60, "31–60 Days")
         .when(F.col("Lead_Time_Days") <= 90, "61–90 Days")
         .otherwise(">90 Days")
    )
    .groupBy("Lead_Time_Band")
    .agg(
        F.countDistinct("Material_ID").alias("Total_Materials")
    )
)

critical_lead_time = (
    final_stockout_df
    .filter(F.col("Stockout_Status") == "Critical Stockout")
    .withColumn(
        "Lead_Time_Band",
        F.when(F.col("Lead_Time_Days") <= 30, "≤30 Days")
         .when(F.col("Lead_Time_Days") <= 60, "31–60 Days")
         .when(F.col("Lead_Time_Days") <= 90, "61–90 Days")
         .otherwise(">90 Days")
    )
    .groupBy("Lead_Time_Band")
    .agg(
        F.countDistinct("Material_ID").alias("Critical_Stockouts")
    )
)

lead_time_rate = (
    material_lead_time
    .join(critical_lead_time, "Lead_Time_Band", "left")
    .fillna(0, ["Critical_Stockouts"])
    .withColumn(
        "Critical_Stockout_Rate_Pct",
        F.round(
            F.col("Critical_Stockouts") /
            F.col("Total_Materials") * 100,
            1
        )
    )
    .orderBy("Lead_Time_Band")
)

display(lead_time_rate)

# COMMAND ----------

# MAGIC %md
# MAGIC Lead time was investigated but did not show a consistent relationship with critical stockout risk.

# COMMAND ----------

# 14.3 Check Demand and Forecast Coverage (Forecast Performance)

from pyspark.sql import functions as F

actuals_df = spark.table("demand_actuals_raw")
forecast_df = spark.table("demand_forecast_raw")

print("Demand Actuals Columns:")
print(actuals_df.columns)

print("\nDemand Forecast Columns:")
print(forecast_df.columns)

print("\nActuals Date Range:")
actuals_df.select(
    F.min("Demand_Month").alias("Min_Month"),
    F.max("Demand_Month").alias("Max_Month")
).show()

print("\nForecast Date Range:")
forecast_df.select(
    F.min("Forecast_Month").alias("Min_Month"),
    F.max("Forecast_Month").alias("Max_Month")
).show()

# COMMAND ----------

# 14.4 Demand Signal Gap Analysis

from pyspark.sql import functions as F

# Reload required datasets
final_stockout_df = spark.table("stockout_risk_powerbi")
forecast_df = spark.table("demand_forecast_raw")

print("Final Stockout Materials:", final_stockout_df.count())



demand_signal = (
    forecast_df
    .groupBy("Material_ID")
    .agg(
        F.sum("Current_Forecast_Units").alias("Current_Forecast"),
        F.sum("Customer_Demand_Units").alias("Customer_Demand"),
        F.sum("Prior_Cycle_Forecast_Units").alias("Prior_Cycle_Forecast")
    )
    .withColumn(
        "Customer_Demand_Gap",
        F.col("Customer_Demand") - F.col("Current_Forecast")
    )
    .withColumn(
        "Forecast_Change",
        F.col("Current_Forecast") - F.col("Prior_Cycle_Forecast")
    )
)

critical_demand_signal = (
    final_stockout_df
    .filter(F.col("Stockout_Status") == "Critical Stockout")
    .select("Material_ID")
    .distinct()
    .join(demand_signal, "Material_ID", "left")
)

display(
    critical_demand_signal.orderBy(F.desc("Customer_Demand_Gap"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Current Forecast is higher than Customer Demand for every critical material shown. Current forecast is NOT driving the critical stockouts

# COMMAND ----------

# 14.5 Forecast Change vs Critical Stockout

from pyspark.sql import functions as F

# Reload required datasets
forecast_df = spark.table("demand_forecast_raw")
final_stockout_df = spark.table("stockout_risk_powerbi")

all_forecast_signal = (
    forecast_df
    .groupBy("Material_ID")
    .agg(
        F.sum("Current_Forecast_Units").alias("Current_Forecast"),
        F.sum("Prior_Cycle_Forecast_Units").alias("Prior_Cycle_Forecast")
    )
    .withColumn(
        "Forecast_Change",
        F.col("Current_Forecast") - F.col("Prior_Cycle_Forecast")
    )
    .withColumn(
        "Forecast_Change_Pct",
        F.when(
            F.col("Prior_Cycle_Forecast") > 0,
            F.round(
                F.col("Forecast_Change") /
                F.col("Prior_Cycle_Forecast") * 100,
                1
            )
        )
    )
)

critical_ids = (
    final_stockout_df
    .filter(F.col("Stockout_Status") == "Critical Stockout")
    .select("Material_ID")
    .distinct()
    .withColumn("Risk_Group", F.lit("Critical Stockout"))
)

forecast_driver_test = (
    all_forecast_signal
    .join(critical_ids, "Material_ID", "left")
    .fillna("Other Materials", ["Risk_Group"])
    .groupBy("Risk_Group")
    .agg(
        F.countDistinct("Material_ID").alias("Material_Count"),
        F.round(F.avg("Forecast_Change_Pct"), 1).alias("Avg_Forecast_Change_Pct"),
        F.round(
            F.expr("percentile_approx(Forecast_Change_Pct, 0.5)"), 1
        ).alias("Median_Forecast_Change_Pct")
    )
)

display(forecast_driver_test)

# COMMAND ----------

# 14.6 Check Available Supply Coverage Fields


from pyspark.sql import functions as F

final_stockout_df = spark.table("stockout_risk_powerbi")

print(final_stockout_df.columns)



from pyspark.sql import functions as F

final_stockout_df = spark.table("stockout_risk_powerbi")

print(final_stockout_df.columns)

# COMMAND ----------

# 14.7 Check Supply Scenario Source Fields

from pyspark.sql import functions as F

supply_df = spark.table("stockout_risk_analysis")

print(supply_df.columns)

# COMMAND ----------

# 14.8 Supply Coverage Driver Analysis

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Reload source tables
supply_df = spark.table("stockout_risk_analysis")
final_stockout_df = spark.table("stockout_risk_powerbi")

# Critical material IDs
critical_ids = (
    final_stockout_df
    .filter(F.col("Stockout_Status") == "Critical Stockout")
    .select("Material_ID")
    .distinct()
)

# Monthly net movement after all identified usable supply
coverage_df = (
    supply_df
    .join(critical_ids, "Material_ID", "inner")
    .withColumn(
        "Net_Movement_All_Supply",
        F.col("Confirmed_Supply")
        + F.col("Planned_Supply")
        + F.col("At_Risk_Supply")
        - F.col("Forecast_Units")
    )
)

# Rolling cumulative movement by material/site
w = (
    Window
    .partitionBy("Material_ID", "Site_ID")
    .orderBy("Month")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

coverage_df = (
    coverage_df
    .withColumn(
        "Projected_Inventory_All_Supply",
        F.first("Starting_Inventory").over(w)
        + F.sum("Net_Movement_All_Supply").over(w)
    )
)

# Summarize remaining exposure by material
supply_coverage_summary = (
    coverage_df
    .groupBy("Material_ID", "Site_ID")
    .agg(
        F.min("Projected_Inventory_All_Supply")
         .alias("Lowest_Projected_Inventory"),
        F.sum("Forecast_Units")
         .alias("Six_Month_Forecast"),
        F.sum("Confirmed_Supply")
         .alias("Confirmed_Supply"),
        F.sum("Planned_Supply")
         .alias("Planned_Supply"),
        F.sum("At_Risk_Supply")
         .alias("At_Risk_Supply")
    )
    .withColumn(
        "Remaining_Shortfall_Units",
        F.when(
            F.col("Lowest_Projected_Inventory") < 0,
            -F.col("Lowest_Projected_Inventory")
        ).otherwise(0)
    )
    .orderBy(F.desc("Remaining_Shortfall_Units"))
)

display(supply_coverage_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC The residual critical stockouts are characterized by insufficient identified supply coverage relative to forecast requirements, even after Planned and At-Risk Supply are considered.

# COMMAND ----------

# 14.9 Critical Stockout Shortfall Concentration

from pyspark.sql import functions as F
from pyspark.sql.window import Window

total_shortfall = (
    supply_coverage_summary
    .agg(F.sum("Remaining_Shortfall_Units").alias("Total"))
    .first()["Total"]
)

pareto_window = (
    Window
    .orderBy(F.desc("Remaining_Shortfall_Units"))
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

shortfall_pareto = (
    supply_coverage_summary
    .withColumn(
        "Shortfall_Share_Pct",
        F.round(
            F.col("Remaining_Shortfall_Units") / F.lit(total_shortfall) * 100,
            1
        )
    )
    .withColumn(
        "Cumulative_Shortfall_Units",
        F.sum("Remaining_Shortfall_Units").over(pareto_window)
    )
    .withColumn(
        "Cumulative_Shortfall_Pct",
        F.round(
            F.col("Cumulative_Shortfall_Units") / F.lit(total_shortfall) * 100,
            1
        )
    )
)

print("Total Remaining Shortfall Units:", total_shortfall)

display(
    shortfall_pareto.select(
        "Material_ID",
        "Site_ID",
        "Remaining_Shortfall_Units",
        "Shortfall_Share_Pct",
        "Cumulative_Shortfall_Pct"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Residual stockout exposure is concentrated but not limited to a small handful of materials: 21 of the 57 critical materials account for approximately 81% of remaining shortfall units.

# COMMAND ----------

## Driver Investigation Summary

Driver investigation was performed to understand whether selected demand and material characteristics distinguish the remaining critical stockouts.

- **Lead Time:** No consistent relationship was observed between longer lead times and critical stockout incidence. Critical stockout rates ranged from 9.3% to 13.1% across lead-time bands, with materials above 90 days showing the lowest rate at 9.3%.

- **Demand Signal:** Customer demand did not exceed the current forecast among the 57 critical materials, indicating that customer demand above forecast was not a common driver of the remaining exposure.

- **Forecast Change:** Critical stockout materials showed an average forecast change of 3.0% versus 2.7% for other materials, with median changes of 2.4% and 2.7%, respectively. The similarity does not indicate a meaningful difference in recent forecast movement.

- **Supply Coverage:** The 57 critical materials remain projected to stock out even after confirmed, planned, and at-risk supply are considered. Remaining shortfalls range from 1 to 608 units by material.

- **Shortfall Concentration:** Approximately 81% of the remaining shortfall units are concentrated across 21 of the 57 critical materials, providing a focused group for supply recovery and mitigation actions.

### Conclusion

The tested lead-time and demand/forecast factors did not show a strong distinguishing relationship with critical stockout risk. The remaining exposure is characterized by insufficient identified supply coverage relative to forecast requirements. Prioritization should therefore focus on materials contributing the largest share of the residual shortfall.

# COMMAND ----------

# MAGIC %md
# MAGIC No single underlying root cause could be established from the available datasets. The tested lead-time and forecast factors did not materially distinguish critical stockouts. Scenario analysis showed that additional supply coverage materially reduces exposure, while Pareto analysis identified 21 materials accounting for approximately 81% of the residual shortfall. These materials provide the priority population for deeper supply-side root-cause investigation.

# COMMAND ----------

## 15. Create Final Power BI Dataset

# 15.1 Build Final Power BI Dataset

from pyspark.sql import functions as F

# Reload final stockout dataset
final_stockout_df = spark.table("stockout_risk_powerbi")

# Add remaining shortfall from driver analysis
shortfall_for_bi = (
    supply_coverage_summary
    .select(
        "Material_ID",
        "Site_ID",
        "Remaining_Shortfall_Units"
    )
)

powerbi_final_df = (
    final_stockout_df
    .join(
        shortfall_for_bi,
        ["Material_ID", "Site_ID"],
        "left"
    )
    .fillna(0, ["Remaining_Shortfall_Units"])
)

print("Final Power BI Rows:", powerbi_final_df.count())

display(powerbi_final_df)

# COMMAND ----------

# 15.2 Final QA and Save

from pyspark.sql import functions as F

print("=== FINAL POWER BI DATASET QA ===")

print("Total Stockout Materials:",
      powerbi_final_df.count())

print("Critical Stockouts:",
      powerbi_final_df
      .filter(F.col("Stockout_Status") == "Critical Stockout")
      .count())

print("Remaining Stockouts:",
      powerbi_final_df
      .filter(F.col("Stockout_Status") == "Stockout")
      .count())

print("Total Remaining Shortfall Units:",
      powerbi_final_df
      .agg(F.sum("Remaining_Shortfall_Units"))
      .first()[0])

print("Null Material IDs:",
      powerbi_final_df
      .filter(F.col("Material_ID").isNull())
      .count())

# Save final reporting table
(
    powerbi_final_df
    .write
    .mode("overwrite")
    .saveAsTable("stockout_risk_powerbi_final")
)

print("\nSaved as: stockout_risk_powerbi_final")

# COMMAND ----------

# 15.3 Display Final Dataset for Export

final_export_df = spark.table("stockout_risk_powerbi_final")

display(final_export_df)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC