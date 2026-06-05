"""
Reusable Airflow helper for batch ingestion via Trino federation.

Pattern:
1. Create BigQuery temp table.
2. Insert source rows from Trino catalog to temp table.
3. Sync final table schema.
4. MERGE temp table into final table.
5. Drop temp table.

This helper is intentionally generic: source_catalog can be postgresql or mysql.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

BQ_TO_TRINO_CAST: dict[str, str] = {
    "INTEGER": "BIGINT",
    "INT64": "BIGINT",
    "STRING": "VARCHAR",
    "BOOL": "BOOLEAN",
    "BOOLEAN": "BOOLEAN",
    "NUMERIC": "DECIMAL(38,9)",
    "BIGNUMERIC": "DECIMAL(38,9)",
    "DATE": "DATE",
}

TRINO_TIMESTAMP_TYPE = "TIMESTAMP(6) WITH TIME ZONE"
METADATA_NAMES: list[str] = ["_ingested_at", "_source_system"]


def build_metadata_exprs(source_system: str) -> list[str]:
    safe_source = source_system.replace("'", "''")
    return [
        f"CAST(CURRENT_TIMESTAMP AS {TRINO_TIMESTAMP_TYPE}) AS _ingested_at",
        f"CAST('{safe_source}' AS VARCHAR) AS _source_system",
    ]


def build_schema_lookup(schema_fields: list[dict[str, Any]]) -> dict[str, str]:
    return {field["name"]: field["type"].upper() for field in schema_fields}


def parse_columns(table_columns_str: str, schema_lookup: dict[str, str]) -> list[str]:
    columns: list[str] = []
    for raw_col in table_columns_str.replace("\n", ",").split(","):
        col = raw_col.strip()
        if not col:
            continue
        if col not in schema_lookup:
            raise ValueError(f"Column '{col}' is not found in schema_fields.")
        if col in METADATA_NAMES:
            raise ValueError(f"Column '{col}' must not be listed in table_columns.")
        columns.append(col)
    if not columns:
        raise ValueError("table_columns is empty after parsing.")
    return columns


def normalize_key_list(merge_key: str | list[str]) -> list[str]:
    if isinstance(merge_key, str):
        keys = [merge_key.strip()]
    else:
        keys = [key.strip() for key in merge_key]
    keys = [key for key in keys if key]
    if not keys:
        raise ValueError("merge_key must contain at least one column.")
    return keys


def build_partition_by_expr(merge_key: str | list[str], prefix: str | None = None) -> str:
    keys = normalize_key_list(merge_key)
    return ", ".join(f"{prefix}.{key}" if prefix else key for key in keys)


def build_merge_condition(merge_key: str | list[str]) -> str:
    keys = normalize_key_list(merge_key)
    return " AND ".join(f"T.{key} = S.{key}" for key in keys)


def build_trino_columns(columns: list[str], schema_lookup: dict[str, str], source_tz: str) -> list[str]:
    expressions: list[str] = []
    for col in columns:
        bq_type = schema_lookup[col].upper()
        if bq_type in {"TIMESTAMP", "DATETIME"}:
            expressions.append(
                f"CAST(src.{col} AT TIME ZONE '{source_tz}' AS {TRINO_TIMESTAMP_TYPE}) AS {col}"
            )
        elif bq_type in BQ_TO_TRINO_CAST:
            expressions.append(f"CAST(src.{col} AS {BQ_TO_TRINO_CAST[bq_type]}) AS {col}")
        else:
            expressions.append(f"src.{col}")
    return expressions


def build_trino_insert_sql(
    *,
    source_catalog: str,
    source_schema: str,
    source_table: str,
    trino_bq_catalog: str,
    bq_dataset: str,
    bq_temp_table: str,
    merge_key: str | list[str],
    partition_field: str,
    columns: list[str],
    trino_columns: list[str],
    metadata_exprs: list[str],
    source_tz: str,
) -> str:
    all_names = columns + METADATA_NAMES
    all_exprs = trino_columns + metadata_exprs
    insert_cols = ",\n        ".join(all_names)
    select_exprs = ",\n            ".join(all_exprs)
    final_cols = ", ".join(all_names)
    partition_by = build_partition_by_expr(merge_key, prefix="src")

    return f"""
INSERT INTO {trino_bq_catalog}.{bq_dataset}.{bq_temp_table} (
    {insert_cols}
)
WITH ranked AS (
    SELECT
        {select_exprs},
        ROW_NUMBER() OVER (
            PARTITION BY {partition_by}
            ORDER BY src.{partition_field} DESC
        ) AS _rn
    FROM {source_catalog}.{source_schema}.{source_table} AS src
    WHERE src.{partition_field} >= TIMESTAMP '{{{{ dag_run.conf.get("window_start") or data_interval_start.in_timezone("{source_tz}").strftime("%Y-%m-%d %H:%M:%S") }}}}'
      AND src.{partition_field} <  TIMESTAMP '{{{{ dag_run.conf.get("window_end")   or data_interval_end.in_timezone("{source_tz}").strftime("%Y-%m-%d %H:%M:%S") }}}}'
)
SELECT {final_cols}
FROM ranked
WHERE _rn = 1
""".strip()


def build_table_resource(*, bq_project: str, bq_dataset: str, table_id: str, schema_fields: list[dict[str, Any]], partition_field: str, partition_type: str, cluster_fields: list[str], labels: dict[str, str]) -> dict[str, Any]:
    resource: dict[str, Any] = {
        "tableReference": {"projectId": bq_project, "datasetId": bq_dataset, "tableId": table_id},
        "schema": {"fields": schema_fields},
        "timePartitioning": {"type": partition_type, "field": partition_field},
    }
    if cluster_fields:
        resource["clustering"] = {"fields": cluster_fields}
    if labels:
        resource["labels"] = labels
    return resource


def build_bq_merge_query(*, bq_project: str, bq_dataset: str, bq_final_table: str, bq_temp_table: str, merge_key: str | list[str], partition_field: str, columns: list[str], append_only: bool, job_labels: dict[str, str]) -> dict[str, Any]:
    key_columns = set(normalize_key_list(merge_key))
    all_cols = columns + METADATA_NAMES
    insert_cols = ", ".join(all_cols)
    insert_vals = ", ".join(f"S.{col}" for col in all_cols)
    partition_by = build_partition_by_expr(merge_key)
    merge_condition = build_merge_condition(merge_key)

    when_matched_clause = ""
    if not append_only:
        update_cols = [col for col in all_cols if col not in key_columns]
        set_clause = ",\n                        ".join(f"T.{col} = S.{col}" for col in update_cols)
        when_matched_clause = f"""
                WHEN MATCHED AND S.{partition_field} >= T.{partition_field} THEN
                    UPDATE SET
                        {set_clause}
"""

    return {
        "query": {
            "query": f"""
                MERGE `{bq_project}.{bq_dataset}.{bq_final_table}` AS T
                USING (
                    SELECT * EXCEPT(_rn)
                    FROM (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY {partition_field} DESC) AS _rn
                        FROM `{bq_project}.{bq_dataset}.{bq_temp_table}`
                    )
                    WHERE _rn = 1
                ) AS S
                ON {merge_condition}
{when_matched_clause}
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT ({insert_cols})
                    VALUES ({insert_vals})
            """,
            "useLegacySql": False,
            "defaultDataset": {"projectId": bq_project, "datasetId": bq_dataset},
        },
        "labels": job_labels,
    }


def sync_final_table_schema(ds_nodash: str, gcp_conn_id: str, bq_project: str, bq_dataset: str, bq_final_table: str, **kwargs: Any) -> str:
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    client = hook.get_client(project_id=bq_project)
    temp_ref = f"{bq_project}.{bq_dataset}.{bq_final_table}_temp_{ds_nodash}"
    final_ref = f"{bq_project}.{bq_dataset}.{bq_final_table}"
    temp_table = client.get_table(temp_ref)
    try:
        final_table = client.get_table(final_ref)
    except Exception:
        logging.info("Final table %s does not exist yet; create task should create it before merge.", final_ref)
        return "final_table_created_or_pending"

    final_columns = {field.name for field in final_table.schema}
    new_columns = [field for field in temp_table.schema if field.name not in final_columns]
    if not new_columns:
        return "no_changes"
    for col in new_columns:
        alter_sql = f"ALTER TABLE `{final_ref}` ADD COLUMN IF NOT EXISTS `{col.name}` {col.field_type}"
        client.query(alter_sql).result(timeout=300)
    return "added:" + ",".join(col.name for col in new_columns)


def normalize_label_part(value: str) -> str:
    normalized = value.lower().replace("_", "-")
    normalized = re.sub(r"[^a-z0-9_-]", "-", normalized)
    normalized = re.sub(r"-+", "-", normalized).strip("-_")
    return normalized[:63] or "unknown"


def build_effective_labels(dag_labels: dict[str, str] | None, table_labels: dict[str, str] | None, source_table: str, source_system: str) -> dict[str, str]:
    merged = {**(dag_labels or {}), "table": source_table, "source-system": source_system, **(table_labels or {})}
    return {normalize_label_part(str(k)): normalize_label_part(str(v)) for k, v in merged.items()}


from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateTableOperator, BigQueryDeleteTableOperator, BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


@dataclass
class TableConfig:
    source_table: str
    bq_final_table: str
    merge_key: str | list[str]
    partition_field: str
    schema_fields: list[dict[str, Any]]
    table_columns: str
    source_catalog: str
    source_schema: str
    source_system: str
    partition_type: str = "DAY"
    cluster_fields: list[str] = field(default_factory=list)
    append_only: bool = False
    labels: dict[str, str] = field(default_factory=dict)


def make_table_task_group(cfg: TableConfig, *, bq_project: str, bq_dataset: str, bq_location: str, trino_conn_id: str, gcp_conn_id: str, trino_bq_cat: str, source_tz: str = "Asia/Jakarta", dag_labels: dict[str, str] | None = None) -> TaskGroup:
    schema_lookup = build_schema_lookup(cfg.schema_fields)
    columns = parse_columns(cfg.table_columns, schema_lookup)
    trino_columns = build_trino_columns(columns, schema_lookup, source_tz)
    metadata_exprs = build_metadata_exprs(cfg.source_system)
    effective_labels = build_effective_labels(dag_labels, cfg.labels, cfg.source_table, cfg.source_system)
    bq_temp_table = f"{cfg.bq_final_table}_temp_{{{{ ds_nodash }}}}"

    with TaskGroup(group_id=f"load_{cfg.source_table}") as task_group:
        create_final = BigQueryCreateTableOperator(
            task_id="create_bq_final_table_if_missing",
            gcp_conn_id=gcp_conn_id,
            project_id=bq_project,
            dataset_id=bq_dataset,
            table_id=cfg.bq_final_table,
            table_resource=build_table_resource(
                bq_project=bq_project, bq_dataset=bq_dataset, table_id=cfg.bq_final_table,
                schema_fields=cfg.schema_fields, partition_field=cfg.partition_field,
                partition_type=cfg.partition_type, cluster_fields=cfg.cluster_fields, labels=effective_labels,
            ),
            if_exists="ignore",
        )

        create_temp = BigQueryCreateTableOperator(
            task_id="create_bq_temp_table",
            gcp_conn_id=gcp_conn_id,
            project_id=bq_project,
            dataset_id=bq_dataset,
            table_id=bq_temp_table,
            table_resource=build_table_resource(
                bq_project=bq_project, bq_dataset=bq_dataset, table_id=bq_temp_table,
                schema_fields=cfg.schema_fields, partition_field=cfg.partition_field,
                partition_type=cfg.partition_type, cluster_fields=cfg.cluster_fields, labels=effective_labels,
            ),
            if_exists="ignore",
        )

        insert_to_temp = SQLExecuteQueryOperator(
            task_id="insert_source_to_bq_temp",
            conn_id=trino_conn_id,
            sql=build_trino_insert_sql(
                source_catalog=cfg.source_catalog,
                source_schema=cfg.source_schema,
                source_table=cfg.source_table,
                trino_bq_catalog=trino_bq_cat,
                bq_dataset=bq_dataset,
                bq_temp_table=bq_temp_table,
                merge_key=cfg.merge_key,
                partition_field=cfg.partition_field,
                columns=columns,
                trino_columns=trino_columns,
                metadata_exprs=metadata_exprs,
                source_tz=source_tz,
            ),
            autocommit=True,
            do_xcom_push=False,
        )

        sync_schema = PythonOperator(
            task_id="sync_final_table_schema",
            python_callable=sync_final_table_schema,
            op_kwargs={"gcp_conn_id": gcp_conn_id, "bq_project": bq_project, "bq_dataset": bq_dataset, "bq_final_table": cfg.bq_final_table},
        )

        merge_to_final = BigQueryInsertJobOperator(
            task_id="merge_temp_to_final",
            gcp_conn_id=gcp_conn_id,
            location=bq_location,
            configuration=build_bq_merge_query(
                bq_project=bq_project, bq_dataset=bq_dataset, bq_final_table=cfg.bq_final_table,
                bq_temp_table=bq_temp_table, merge_key=cfg.merge_key, partition_field=cfg.partition_field,
                columns=columns, append_only=cfg.append_only, job_labels=effective_labels,
            ),
        )

        drop_temp = BigQueryDeleteTableOperator(
            task_id="drop_bq_temp_table",
            gcp_conn_id=gcp_conn_id,
            deletion_dataset_table=f"{bq_project}.{bq_dataset}.{bq_temp_table}",
            ignore_if_missing=True,
            trigger_rule=TriggerRule.ALL_DONE,
        )

        create_final >> create_temp >> insert_to_temp >> sync_schema >> merge_to_final >> drop_temp

    return task_group
