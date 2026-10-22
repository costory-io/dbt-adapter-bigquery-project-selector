from importlib import import_module

from dbt.adapters.base.relation import AdapterTrackingRelationInfo
from dbt.adapters.bigquery.impl import BigQueryAdapter
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.bigquery_cost_optimization.connections import (
    BigQueryCostOptimizationConnectionManager,
)


class BigQueryCostOptimizationAdapter(BigQueryAdapter):
    ConnectionManager = BigQueryCostOptimizationConnectionManager

    @classmethod
    def get_adapter_run_info(
        cls, config: RelationConfig
    ) -> AdapterTrackingRelationInfo:
        # BaseAdapter derives the module from the class name
        # (BigQueryCostOptimizationAdapter → "bigquerycostoptimization").
        # The package path uses underscores, so override the lookup.
        return AdapterTrackingRelationInfo(
            adapter_name="bigquery_cost_optimization",
            base_adapter_version=import_module("dbt.adapters.__about__").version,
            adapter_version=import_module(
                "dbt.adapters.bigquery_cost_optimization.__version__"
            ).version,
            model_adapter_details=cls._get_adapter_specific_run_info(config),
        )
