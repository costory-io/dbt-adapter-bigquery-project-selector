# dbt-bigquery-cost-optimization

A dbt adapter derived from `dbt-bigquery`. Install it next to `dbt-core` and set the profile target type to `bigquery_cost_optimization`.

If the node has a tag listed in `execution_projects_by_tag`, the job runs in that GCP project. The destination project in the compiled SQL does not change.

## Install

```bash
pip install dbt-bigquery-cost-optimization
```

## Profile

```yaml
my_project:
  target: prod
  outputs:
    prod:
      type: bigquery_cost_optimization
      method: oauth
      project: analytics-prod
      execution_project: analytics-prod
      dataset: dbt
      location: EU
      threads: 8
      execution_projects_by_tag:
        reservation: analytics-prod-reservation
        backfill: analytics-prod-backfill
```

`execution_projects_by_tag` maps a dbt tag to the GCP project that should own and bill the query job. Tags that are not listed keep the target `execution_project`.

A model opts in with a tag:

```sql
{{ config(tags=["reservation"]) }}
```

## Routing rules

- No configured tag on the node: the job stays on `execution_project`.
- One configured tag: the job is submitted to that tag's project.
- Several configured tags that point at different projects: the run fails.
- Several configured tags that point at the same project: the job uses that project.

## Job labels

The adapter does not add job labels. To label jobs, set `query-comment.job-label: true` in `dbt_project.yml`; dbt-bigquery then copies the query comment into the job labels, and those labels are kept when a job is routed to another project.
