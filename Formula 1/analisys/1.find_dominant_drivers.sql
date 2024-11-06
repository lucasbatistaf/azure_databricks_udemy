-- Databricks notebook source
SELECT 
  driver_name
  , COUNT(1) AS total_races
  , SUM(calculated_points) AS total_points
  , AVG(calculated_points) AS avg_points
FROM
  f1_presentation.calculated_race_results
WHERE 1=1
  AND race_year BETWEEN 2001 AND 2010
GROUP BY
  driver_name
HAVING
  COUNT(1) >= 50
ORDER BY 
  avg_points DESC

-- COMMAND ----------

