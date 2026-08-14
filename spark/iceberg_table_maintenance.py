#!/usr/bin/env python3
"""Spark driver used by Rivus Iceberg table-maintenance submissions."""

import argparse
import json
import sys

from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser(description="Run validated Rivus Iceberg maintenance procedures")
    parser.add_argument("--payload-json", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    payload = json.loads(args.payload_json)
    statements = payload.get("statements")
    if not isinstance(statements, list) or not statements:
        raise ValueError("maintenance payload has no statements")

    spark = SparkSession.builder.getOrCreate()
    results = []
    try:
        for statement in statements:
            sql = statement.get("sql")
            if not isinstance(sql, str) or not sql.startswith("CALL "):
                raise ValueError("maintenance payload contains an invalid statement")
            rows = [row.asDict(recursive=True) for row in spark.sql(sql).collect()]
            result = {
                "operation": statement.get("operation"),
                "table": statement.get("table"),
                "rows": rows,
            }
            results.append(result)
            print(json.dumps({"rivus_iceberg_maintenance": result}, default=str), flush=True)
    finally:
        spark.stop()

    print(json.dumps({"rivus_iceberg_maintenance_complete": results}, default=str), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(json.dumps({"rivus_iceberg_maintenance_error": str(error)}), file=sys.stderr, flush=True)
        raise
