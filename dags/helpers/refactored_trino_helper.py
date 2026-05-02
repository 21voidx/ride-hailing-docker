"""
helpers/trino_helper.py

Reusable helper for Airflow ingestion pipeline:
PostgreSQL -> BigQuery via Trino cross-catalog.

Layer 1 contains pure utilities and has no Airflow imports.
Layer 2 contains Airflow TaskGroup factory.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any


# =============================================================================
# Layer 1: Pure utilities
# =============================================================================

BQ_TO_TRINO_CAST: dict[str, str] = {
    "INTEGER": "BIGINT",
    "INT64": "BIGINT",
    "STRING": "VARCHAR",
    "BOOL": "BOOLEAN",
    "BOOLEAN": "BOOLEAN",
    "NUMERIC": "DECIMAL(38,9)",
    "BIGNUMERIC": "DECIMAL(38,9)",
}

TRINO_TIMESTAMP_TYPE = "TIMESTAMP(6) WITH TIME ZONE"
METADATA_NAMES: list[str] = ["_ingested_at", "_source_system"]


def build_metadata_exprs(source_system: str) -> list[str]:
    """Build metadata expressions used in Trino SELECT."""
    safe_source = source_system.replace("'", "''")
    return [
        f"CAST(CURRENT_TIMESTAMP AS {TRINO_TIMESTAMP_TYPE}) AS _ingested_at",
        f"CAST('{safe_source}' AS VARCHAR) AS _source_system",
    ]


def build_schema_lookup(schema_fields: list[dict[str, Any]]) -> dict[str, str]:
    """Return {column_name: BQ type} from BigQuery schema fields."""
    return {field["name"]: field["type"].upper() for field in schema_fields}


def parse_columns(table_columns_str: str, schema_lookup: dict[str, str]) -> list[str]:
    """Parse table_columns and validate each column against schema_fields."""
    columns: list[str] = []

    for raw_col in table_columns_str.replace("\n", ",").split(","):
        col = raw_col.strip()

        if not col:
            continue

        if col not in schema_lookup:
            raise ValueError(
                f"Column '{col}' is not found in schema_fields. "
                "Fix table_columns or add the column to schema_fields."
            )

        if col in METADATA_NAMES:
            raise ValueError(
                f"Column '{col}' must not be listed in table_columns. "
                "Metadata columns are added automatically."
            )

        columns.append(col)

    if not columns:
        raise ValueError("table_columns is empty after parsing.")

    return columns


def normalize_key_list(merge_key: str | list[str]) -> list[str]:
    """Normalize merge_key into a non-empty list."""
    if isinstance(merge_key, str):
        keys = [merge_key.strip()]
    else:
        keys = [key.strip() for key in merge_key]

    keys = [key for key in keys if key]

    if not keys:
        raise ValueError("merge_key must contain at least one column.")

    return keys


def build_partition_by_expr(merge_key: str | list[str], prefix: str | None = None) -> str:
    """Build PARTITION BY expression for single or composite keys."""
    keys = normalize_key_list(merge_key)

    if prefix:
        return ", ".join(f"{prefix}.{key}" for key in keys)

    return ", ".join(keys)


def build_merge_condition(merge_key: str | list[str]) -> str:
    """Build BigQuery MERGE ON condition for single or composite keys."""
    keys = normalize_key_list(merge_key)
    return " AND ".join(f"T.{key} = S.{key}" for key in keys)


def build_trino_columns(
    columns: list[str],
    schema_lookup: dict[str, str],
    json_columns: list[str] | None = None,
    source_tz: str = "Asia/Jakarta",
) -> list[str]:
    """Build Trino SELECT expressions with source to BigQuery type normalization."""
    json_columns = json_columns or []
    json_set = set(json_columns)

    expressions: list[str] = []

    for col in columns:
        bq_type = schema_lookup[col].upper()

        if col in json_set:
            expressions.append(f"json_format(src.{col}) AS {col}")
        elif bq_type in {"TIMESTAMP", "DATETIME"}:
            expressions.append(
                f"CAST(src.{col} AT TIME ZONE '{source_tz}' "
                f"AS {TRINO_TIMESTAMP_TYPE}) AS {col}"
            )
        elif bq_type in BQ_TO_TRINO_CAST:
            expressions.append(f"CAST(src.{col} AS {BQ_TO_TRINO_CAST[bq_type]}) AS {col}")
        else:
            expressions.append(f"src.{col}")

    return expressions


def build_trino_insert_sql(
    *,
    trino_bq_catalog: str,
    trino_pg_catalog: str,
    bq_dataset: str,
    bq_temp_table: str,
    pg_schema: str,
    pg_source_table: str,
    merge_key: str | list[str],
    partition_field: str,
    columns: list[str],
    trino_columns: list[str],
    metadata_exprs: list[str],
    source_tz: str = "Asia/Jakarta",
) -> str:
    """Build Trino INSERT SQL from source table into BigQuery temp table."""
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
    FROM {trino_pg_catalog}.{pg_schema}.{pg_source_table} AS src
    WHERE src.{partition_field} >= TIMESTAMP '{{{{ dag_run.conf.get("window_start") or data_interval_start.in_timezone("{source_tz}").strftime("%Y-%m-%d %H:%M:%S") }}}}'
      AND src.{partition_field} <  TIMESTAMP '{{{{ dag_run.conf.get("window_end")   or data_interval_end.in_timezone("{source_tz}").strftime("%Y-%m-%d %H:%M:%S") }}}}'
)
SELECT {final_cols}
FROM ranked
WHERE _rn = 1
""".strip()


def build_table_resource(
    *,
    bq_project: str,
    bq_dataset: str,
    table_id: str,
    schema_fields: list[dict[str, Any]],
    partition_field: str,
    partition_type: str = "MONTH",
    cluster_fields: list[str] | None = None,
    labels: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build BigQuery table_resource for BigQueryCreateTableOperator."""
    resource: dict[str, Any] = {
        "tableReference": {
            "projectId": bq_project,
            "datasetId": bq_dataset,
            "tableId": table_id,
        },
        "schema": {"fields": schema_fields},
        "timePartitioning": {
            "type": partition_type,
            "field": partition_field,
        },
    }

    if cluster_fields:
        resource["clustering"] = {"fields": cluster_fields}

    if labels:
        resource["labels"] = labels

    return resource


def build_bq_merge_query(
    *,
    bq_project: str,
    bq_dataset: str,
    bq_final_table: str,
    bq_temp_table: str,
    merge_key: str | list[str],
    partition_field: str,
    columns: list[str],
    append_only: bool = False,
    job_labels: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build BigQuery MERGE job configuration for upsert or append-only mode."""
    key_columns = set(normalize_key_list(merge_key))

    all_cols = columns + METADATA_NAMES
    insert_cols = ", ".join(all_cols)
    insert_vals = ", ".join(f"S.{col}" for col in all_cols)

    partition_by = build_partition_by_expr(merge_key)
    merge_condition = build_merge_condition(merge_key)

    when_matched_clause = ""
    if not append_only:
        update_cols = [col for col in all_cols if col not in key_columns]
        set_clause = ",\n                        ".join(
            f"T.{col} = S.{col}" for col in update_cols
        )

        when_matched_clause = f"""
                WHEN MATCHED AND S.{partition_field} > T.{partition_field} THEN
                    UPDATE SET
                        {set_clause}
"""

    config: dict[str, Any] = {
        "query": {
            "query": f"""
                MERGE `{bq_project}.{bq_dataset}.{bq_final_table}` AS T
                USING (
                    SELECT * EXCEPT(_rn)
                    FROM (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (
                                PARTITION BY {partition_by}
                                ORDER BY {partition_field} DESC
                            ) AS _rn
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
            "defaultDataset": {
                "projectId": bq_project,
                "datasetId": bq_dataset,
            },
        }
    }

    if job_labels:
        config["labels"] = job_labels

    return config


def sync_final_table_schema(
    ds_nodash: str,
    gcp_conn_id: str,
    bq_project: str,
    bq_dataset: str,
    bq_final_table: str,
    **kwargs: Any,
) -> str:
    """Add missing columns from temp table to final table."""
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    client = hook.get_client(project_id=bq_project)

    temp_ref = f"{bq_project}.{bq_dataset}.{bq_final_table}_temp_{ds_nodash}"
    final_ref = f"{bq_project}.{bq_dataset}.{bq_final_table}"

    try:
        temp_table = client.get_table(temp_ref)
        final_table = client.get_table(final_ref)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to read BigQuery schema. "
            f"Check temp table '{temp_ref}' and final table '{final_ref}'. "
            f"Detail: {exc}"
        ) from exc

    final_columns = {field.name for field in final_table.schema}
    new_columns = [field for field in temp_table.schema if field.name not in final_columns]

    if not new_columns:
        logging.info("Schema already synced for %s", final_ref)
        return "no_changes"

    for col in new_columns:
        alter_sql = (
            f"ALTER TABLE `{final_ref}` "
            f"ADD COLUMN IF NOT EXISTS `{col.name}` {col.field_type}"
        )

        try:
            job = client.query(alter_sql)
            job.result(timeout=300)
            logging.info("Added column %s.%s as %s", final_ref, col.name, col.field_type)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to add column '{col.name}' with type '{col.field_type}' "
                f"to '{final_ref}'. Detail: {exc}"
            ) from exc

    added = [col.name for col in new_columns]
    return f"added:{','.join(added)}"


def normalize_label_part(value: str) -> str:
    """Normalize a string into a BigQuery-safe label key or value."""
    normalized = value.lower().replace("_", "-")
    normalized = re.sub(r"[^a-z0-9_-]", "-", normalized)
    normalized = re.sub(r"-+", "-", normalized).strip("-_")
    return normalized[:63] or "unknown"


def build_effective_labels(
    *,
    dag_labels: dict[str, str] | None,
    table_labels: dict[str, str] | None,
    pg_table: str,
    source_system: str,
) -> dict[str, str]:
    """Merge DAG, auto-generated, and table-level labels."""
    merged = {
        **(dag_labels or {}),
        "table": pg_table,
        "source-system": source_system,
        **(table_labels or {}),
    }

    return {
        normalize_label_part(str(key)): normalize_label_part(str(value))
        for key, value in merged.items()
    }


# =============================================================================
# Layer 2: Airflow TaskGroup factory
# =============================================================================

from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateTableOperator,
    BigQueryDeleteTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.sdk.definitions.asset import Asset
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


@dataclass
class TableConfig:
    pg_table: str
    bq_final_table: str
    merge_key: str | list[str]
    partition_field: str
    schema_fields: list[dict[str, Any]]
    table_columns: str
    source_system: str = "source"
    partition_type: str = "MONTH"
    cluster_fields: list[str] = field(default_factory=list)
    append_only: bool = False
    json_fields: list[str] = field(default_factory=list)
    labels: dict[str, str] = field(
        default_factory=lambda: {"env": "dev", "team": "data-eng"}
    )


def make_table_task_group(
    cfg: TableConfig | dict[str, Any],
    *,
    bq_project: str,
    bq_dataset: str,
    bq_location: str,
    pg_schema: str,
    trino_conn_id: str,
    gcp_conn_id: str,
    trino_bq_cat: str,
    trino_pg_cat: str,
    source_tz: str = "Asia/Jakarta",
    dag_labels: dict[str, str] | None = None,
) -> TaskGroup:
    """Create one 5-step TaskGroup for one source table."""
    if isinstance(cfg, dict):
        cfg = TableConfig(**cfg)

    schema_lookup = build_schema_lookup(cfg.schema_fields)
    columns = parse_columns(cfg.table_columns, schema_lookup)
    trino_columns = build_trino_columns(
        columns=columns,
        schema_lookup=schema_lookup,
        json_columns=cfg.json_fields,
        source_tz=source_tz,
    )
    metadata_exprs = build_metadata_exprs(cfg.source_system)

    effective_labels = build_effective_labels(
        dag_labels=dag_labels,
        table_labels=cfg.labels,
        pg_table=cfg.pg_table,
        source_system=cfg.source_system,
    )

    bq_temp_table = f"{cfg.bq_final_table}_temp_{{{{ ds_nodash }}}}"

    bq_final_asset = Asset(f"bigquery://{bq_project}/{bq_dataset}/{cfg.bq_final_table}")
    bq_temp_asset = Asset(f"bigquery://{bq_project}/{bq_dataset}/{bq_temp_table}")

    with TaskGroup(group_id=f"load_{cfg.pg_table}") as task_group:
        create_temp = BigQueryCreateTableOperator(
            task_id="create_bq_temp_table",
            gcp_conn_id=gcp_conn_id,
            project_id=bq_project,
            dataset_id=bq_dataset,
            table_id=bq_temp_table,
            table_resource=build_table_resource(
                bq_project=bq_project,
                bq_dataset=bq_dataset,
                table_id=bq_temp_table,
                schema_fields=cfg.schema_fields,
                partition_field=cfg.partition_field,
                partition_type=cfg.partition_type,
                cluster_fields=cfg.cluster_fields,
                labels=effective_labels,
            ),
            if_exists="ignore",
            outlets=[bq_temp_asset],
        )

        insert_to_temp = SQLExecuteQueryOperator(
            task_id="insert_source_to_bq_temp",
            conn_id=trino_conn_id,
            sql=build_trino_insert_sql(
                trino_bq_catalog=trino_bq_cat,
                trino_pg_catalog=trino_pg_cat,
                bq_dataset=bq_dataset,
                bq_temp_table=bq_temp_table,
                pg_schema=pg_schema,
                pg_source_table=cfg.pg_table,
                merge_key=cfg.merge_key,
                partition_field=cfg.partition_field,
                columns=columns,
                trino_columns=trino_columns,
                metadata_exprs=metadata_exprs,
                source_tz=source_tz,
            ),
            autocommit=True,
            do_xcom_push=False,
            outlets=[bq_temp_asset],
        )

        sync_schema = PythonOperator(
            task_id="sync_final_table_schema",
            python_callable=sync_final_table_schema,
            op_kwargs={
                "gcp_conn_id": gcp_conn_id,
                "bq_project": bq_project,
                "bq_dataset": bq_dataset,
                "bq_final_table": cfg.bq_final_table,
            },
        )

        merge_to_final = BigQueryInsertJobOperator(
            task_id="merge_temp_to_final",
            gcp_conn_id=gcp_conn_id,
            location=bq_location,
            configuration=build_bq_merge_query(
                bq_project=bq_project,
                bq_dataset=bq_dataset,
                bq_final_table=cfg.bq_final_table,
                bq_temp_table=bq_temp_table,
                merge_key=cfg.merge_key,
                partition_field=cfg.partition_field,
                columns=columns,
                append_only=cfg.append_only,
                job_labels=effective_labels,
            ),
            outlets=[bq_final_asset],
        )

        drop_temp = BigQueryDeleteTableOperator(
            task_id="drop_bq_temp_table",
            gcp_conn_id=gcp_conn_id,
            deletion_dataset_table=f"{bq_project}.{bq_dataset}.{bq_temp_table}",
            ignore_if_missing=True,
            trigger_rule=TriggerRule.ALL_DONE,
        )

        create_temp >> insert_to_temp >> sync_schema >> merge_to_final >> drop_temp

    return task_group
