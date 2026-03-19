# Databricks notebook source
{
    "bronze_schema_name": "nmls_mcr_raw",
    "silver_schema_name": "nmls_mcr",
    "gold_schema_name": "nmls_mcr_gold",
    "dms_schema_name": "nmls_mcr_dms",
    "copy_into": [
        {
            "old_schema_name": "mcr",
            "migration_volume_name": "ppas_prod_files_parquet",
            "schema_name": "nmls_mcr_dms"
        },
        {
            "old_schema_name": "mcr",
            "migration_volume_name": "ppas_prod_files_parquet",
            "schema_name": "nmls_mcr_val"
        }
    ],
    "bronze": {
        "tables": [
            "company",
            "filing",
            "line_item_data",
            "line_item_reference",
            "loans_serviced_by_others",
            "loans_serviced_for_others",
            "loans_serviced_under_msrs",
            "loc",
            "mlo_data",
            "questionable_item",
            "transaction_history",
            "file_version_history",
            "table_counts"
        ],
        "custom_sql_commands": ""
    },
    "silver": {
        "tables_in_dms": [
            "company",
            "filing",
            "line_item_data",
            "line_item_reference",
            "loans_serviced_by_others",
            "loans_serviced_for_others",
            "loans_serviced_under_msrs",
            "loc",
            "mlo_data",
            "questionable_item"
        ],
        "tables_with_custom_types": [
            {
                "table_name": "line_item_data",
                "view_name": "line_item_data_dms_view",
                "columns": [
                    {
                        "name": "prior_four_quarter_average",
                        "type": "decimal(38,18)"
                    },
                    {
                        "name": "ratio_of_current_to_prior_4_avg",
                        "type": "decimal(38,18)"
                    }
                ]
            }
        ],
        "tables_not_in_dms": [
            "table_counts"
        ],
        "identity_columns": []
    },
    "gold": {
        "tables": [
            {
                "name": "line_item_filter",
                "columns": [
                    { "name": "line_item", "type": "string" },
                    { "name": "group_by", "type": "string" },
                    { "name": "sequence", "type": "integer" }
                ]
            }
        ]
    }
}