from dbt.adapters.base import AdapterPlugin

from dbt.adapters.bigquery_cost_optimization.connections import (
    BigQueryCostOptimizationConnectionManager,
)
from dbt.adapters.bigquery_cost_optimization.credentials import (
    BigQueryCostOptimizationCredentials,
)
from dbt.adapters.bigquery_cost_optimization.impl import BigQueryCostOptimizationAdapter
from dbt.include import bigquery_cost_optimization

Plugin = AdapterPlugin(
    adapter=BigQueryCostOptimizationAdapter,
    credentials=BigQueryCostOptimizationCredentials,
    include_path=bigquery_cost_optimization.PACKAGE_PATH,
    dependencies=["bigquery"],
)

__all__ = [
    "Plugin",
    "BigQueryCostOptimizationAdapter",
    "BigQueryCostOptimizationConnectionManager",
    "BigQueryCostOptimizationCredentials",
]
