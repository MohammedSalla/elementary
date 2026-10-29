{%- macro get_test_results(days_back = 7, invocations_per_test = 720, disable_passed_test_metrics = false, skip_test_result_rows = false) -%}
    {{ return(adapter.dispatch('get_test_results', 'elementary_cli')(days_back, invocations_per_test, disable_passed_test_metrics, skip_test_result_rows)) }}
{%- endmacro -%}

{#
    Shared post-processing helper: filters tests by meta, attaches sample data.
    Called by both default__ and fabric__ dispatches to avoid duplicating the
    Jinja processing loop.
#}
{%- macro _process_raw_test_results(test_results_agate, test_result_rows_agate, elementary_tests_allowlist_status, skip_test_result_rows = false) -%}
    {% set test_results = [] %}
    {% set tests = elementary.agate_to_dicts(test_results_agate) %}

    {% set filtered_tests = [] %}
    {% for test in tests %}
        {% set test_meta = fromjson(test.meta) %}
        {% if test_meta.get("elementary", {}).get("include", true) %}
            {% do filtered_tests.append(test) %}
        {% endif %}
    {% endfor %}

    {% for test in filtered_tests %}
        {% set test_rows_sample = none %}
        {% if test.invocations_rank_index == 1 %}
            {% set test_type = test.test_type %}
            {% set test_params = fromjson(test.test_params) %}
            {% set status = test.status | lower %}

            {%- if not skip_test_result_rows and ((test_type == 'dbt_test' and status in ['fail', 'warn']) or (test_type != 'dbt_test' and status in elementary_tests_allowlist_status)) -%}
                {% set test_rows_sample = elementary_cli.get_test_rows_sample(test.result_rows, test_result_rows_agate.get(test.id)) %}
            {%- endif -%}
        {% else %}
            {# Null out test_results_query for non-latest invocations to save memory #}
            {% do test.update({"test_results_query": none}) %}
        {% endif %}
        {# Adding sample data to test results #}
        {% do test.update({"sample_data": test_rows_sample}) %}
        {% do test_results.append(test) %}
    {%- endfor -%}
    {% do return(test_results) %}
{%- endmacro -%}

{%- macro default__get_test_results(days_back = 7, invocations_per_test = 720, disable_passed_test_metrics = false, skip_test_result_rows = false) -%}
    {% set elementary_tests_allowlist_status = ['fail', 'warn'] if disable_passed_test_metrics else ['fail', 'warn', 'pass']  %}
    {% set select_test_results %}
        with test_results as (
            {{ elementary_cli.current_tests_run_results_query(days_back=days_back, skip_test_result_rows=skip_test_result_rows) }}
        ),

        ordered_test_results as (
            select
                *,
                {{ elementary.edr_datediff(elementary.edr_cast_as_timestamp('detected_at'), elementary.edr_current_timestamp(), 'day') }} as days_diff,
                {# When we split test into multiple test results, we want to have the same invocation order for the test results from the same run so we use rank. #}
                row_number() over (partition by elementary_unique_id order by {{elementary.edr_cast_as_timestamp('detected_at')}} desc) as invocations_rank_index
            from test_results
        )

        select
            test_results.id,
            test_results.invocation_id,
            test_results.test_execution_id,
            test_results.model_unique_id,
            test_results.test_unique_id,
            test_results.elementary_unique_id,
            {{elementary.edr_cast_as_timestamp('test_results.detected_at')}} as detected_at,
            test_results.database_name,
            test_results.schema_name,
            test_results.table_name,
            test_results.column_name,
            test_results.test_type,
            test_results.test_sub_type,
            test_results.test_results_description,
            test_results.test_description,
            test_results.original_path,
            test_results.package_name,
            test_results.owners,
            test_results.model_owner,
            test_results.tags,
            test_results.test_tags,
            test_results.model_tags,
            test_results.meta,
            test_results.model_meta,
            case when test_results.invocations_rank_index = 1 then test_results.test_results_query else NULL end as test_results_query,
            test_results.other,
            test_results.test_name,
            test_results.test_params,
            test_results.severity,
            test_results.status,
            test_results.execution_time,
            test_results.days_diff,
            test_results.invocations_rank_index,
            test_results.failures,
            test_results.result_rows
        from ordered_test_results as test_results
        where test_results.invocations_rank_index <= {{ invocations_per_test }}
        order by test_results.elementary_unique_id, test_results.invocations_rank_index desc
    {%- endset -%}

    {% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
    {% set ordered_test_results_relation = elementary.create_temp_table(elementary_database, elementary_schema, 'ordered_test_results', select_test_results) %}

    {% set test_results_agate_sql %}
        select * from {{ ordered_test_results_relation }}
    {% endset %}

    {% set valid_ids_query %}
        select distinct id
        from {{ ordered_test_results_relation }}
        where invocations_rank_index = 1
    {% endset %}

    {% set test_results_agate = elementary.run_query(test_results_agate_sql) %}
    {% if not skip_test_result_rows %}
        {% set test_result_rows_agate = elementary_cli.get_result_rows_agate(days_back, valid_ids_query) %}
    {% else %}
        {% set test_result_rows_agate = {} %}
    {% endif %}
    {% if not elementary.has_temp_table_support() %}
        {% do elementary.fully_drop_relation(ordered_test_results_relation) %}
    {% endif %}

    {% do return(elementary_cli._process_raw_test_results(test_results_agate, test_result_rows_agate, elementary_tests_allowlist_status, skip_test_result_rows)) %}
{%- endmacro -%}

{%- macro fabric__get_test_results(days_back = 7, invocations_per_test = 720, disable_passed_test_metrics = false, skip_test_result_rows = false) -%}
    {#
        T-SQL does not allow nested CTEs (WITH inside WITH).
        current_tests_run_results_query already starts with WITH, so we
        cannot wrap it in another CTE.  Instead we materialise it into a
        temp table first, then build ordered_test_results on top.
    #}
    {% set elementary_tests_allowlist_status = ['fail', 'warn'] if disable_passed_test_metrics else ['fail', 'warn', 'pass']  %}

    {# Step 1 – materialise the base test-results query into a temp table #}
    {% set base_query %}
        {{ elementary_cli.current_tests_run_results_query(days_back=days_back, skip_test_result_rows=skip_test_result_rows) }}
    {% endset %}

    {% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
    {% set base_relation = elementary.create_temp_table(elementary_database, elementary_schema, 'test_results_base', base_query) %}

    {# Step 2 – build ordered_test_results from the materialised base (no nested CTE) #}
    {% set select_ordered %}
        select
            *,
            {{ elementary.edr_datediff(elementary.edr_cast_as_timestamp('detected_at'), elementary.edr_current_timestamp(), 'day') }} as days_diff,
            row_number() over (partition by elementary_unique_id order by {{elementary.edr_cast_as_timestamp('detected_at')}} desc) as invocations_rank_index
        from {{ base_relation }}
    {% endset %}

    {% set ordered_relation = elementary.create_temp_table(elementary_database, elementary_schema, 'ordered_test_results', select_ordered) %}

    {# Step 3 – final query: filter by invocations_per_test #}
    {# ORDER BY must be here, not inside create_temp_table — T-SQL forbids ORDER BY in views/subqueries without TOP #}
    {% set test_results_agate_sql %}
        select *
        from {{ ordered_relation }}
        where invocations_rank_index <= {{ invocations_per_test }}
        order by elementary_unique_id, invocations_rank_index desc
    {% endset %}

    {% set valid_ids_query %}
        select distinct id
        from {{ ordered_relation }}
        where invocations_rank_index = 1
    {% endset %}

    {% set test_results_agate = elementary.run_query(test_results_agate_sql) %}
    {% if not skip_test_result_rows %}
        {% set test_result_rows_agate = elementary_cli.get_result_rows_agate(days_back, valid_ids_query) %}
    {% else %}
        {% set test_result_rows_agate = {} %}
    {% endif %}

    {# Clean up intermediate tables #}
    {% do elementary.fully_drop_relation(base_relation) %}
    {% do elementary.fully_drop_relation(ordered_relation) %}

    {% do return(elementary_cli._process_raw_test_results(test_results_agate, test_result_rows_agate, elementary_tests_allowlist_status, skip_test_result_rows)) %}
{%- endmacro -%}

{%- macro sqlserver__get_test_results(days_back = 7, invocations_per_test = 720, disable_passed_test_metrics = false, skip_test_result_rows = false) -%}
    {% do return(elementary_cli.fabric__get_test_results(days_back, invocations_per_test, disable_passed_test_metrics, skip_test_result_rows)) %}
{%- endmacro -%}

{%- macro clickhouse__get_test_results(days_back = 7, invocations_per_test = 720, disable_passed_test_metrics = false, skip_test_result_rows = false) -%}
    {% set elementary_tests_allowlist_status = ['fail', 'warn'] if disable_passed_test_metrics else ['fail', 'warn', 'pass']  %}
    {#
        Read through a subquery instead of staging rows in a scratch table: on a multi-replica
        cluster, a plain MergeTree table only has the rows on the replica that ran the INSERT,
        so a read routed to another replica returned no test results.
        Every column is aliased because ClickHouse keeps the table prefix (e.g. `etr.status`)
        on a column whose name also exists in a joined table.
    #}
    {% set ordered_test_results_query %}
    SELECT
        etr.id AS id,
        etr.invocation_id AS invocation_id,
        etr.test_execution_id AS test_execution_id,
        etr.model_unique_id AS model_unique_id,
        etr.test_unique_id AS test_unique_id,
        CASE
            WHEN etr.test_type = 'schema_change' THEN etr.test_unique_id
            WHEN dt.short_name = 'dimension_anomalies' THEN etr.test_unique_id
            ELSE {{ dbt.concat(["coalesce(etr.test_unique_id, 'None')", "'.'", "coalesce(nullif(etr.column_name, ''), 'None')", "'.'", "coalesce(etr.test_sub_type, 'None')"]) }}
        END AS elementary_unique_id,
        etr.detected_at AS detected_at,
        etr.database_name AS database_name,
        etr.schema_name AS schema_name,
        etr.table_name AS table_name,
        etr.column_name AS column_name,
        etr.test_type AS test_type,
        etr.test_sub_type AS test_sub_type,
        etr.test_results_description AS test_results_description,
        dt.description AS test_description,
        dt.original_path AS original_path,
        dt.package_name AS package_name,
        etr.owners AS owners,
        da.owner AS model_owner,
        etr.tags AS tags,
        dt.tags AS test_tags,
        da.tags AS model_tags,
        dt.meta AS meta,
        da.meta AS model_meta,
        etr.test_results_query AS test_results_query,
        etr.other AS other,
        CASE
            WHEN dt.short_name IS NOT NULL THEN dt.short_name
            ELSE etr.test_name
        END AS test_name,
        etr.test_params AS test_params,
        etr.severity AS severity,
        etr.status AS status,
        drr.execution_time AS execution_time,
        {{ elementary.edr_datediff(elementary.edr_cast_as_timestamp('etr.detected_at'), elementary.edr_current_timestamp(), 'day') }} AS days_diff,
        ROW_NUMBER() OVER (PARTITION BY elementary_unique_id ORDER BY etr.detected_at DESC) AS invocations_rank_index,
        etr.failures AS failures,
        {% if skip_test_result_rows %}''{% else %}etr.result_rows{% endif %} AS result_rows
    FROM {{ ref('elementary', 'elementary_test_results') }} etr
    JOIN {{ ref('elementary', 'dbt_tests') }} dt ON etr.test_unique_id = dt.unique_id
    LEFT JOIN (
        SELECT
            min(detected_at) AS first_time_occurred,
            test_unique_id
        FROM {{ ref('elementary', 'elementary_test_results') }}
        GROUP BY test_unique_id
    ) first_occurred ON etr.test_unique_id = first_occurred.test_unique_id
    LEFT JOIN (
        SELECT unique_id, meta, tags, owner FROM {{ ref('elementary', 'dbt_models') }}
        UNION ALL
        SELECT unique_id, meta, tags, owner FROM {{ ref('elementary', 'dbt_sources') }}
    ) da ON etr.model_unique_id = da.unique_id
    LEFT JOIN {{ ref('elementary', 'dbt_run_results') }} drr ON etr.test_execution_id = drr.model_execution_id
    WHERE {{ elementary.edr_datediff(elementary.edr_cast_as_timestamp('etr.detected_at'), elementary.edr_current_timestamp(), 'day') }} < {{ days_back }}
    {% endset %}

    {% set test_results = [] %}

    {% set test_results_agate_sql %}
        select * from ({{ ordered_test_results_query }}) as ordered_test_results
        order by elementary_unique_id, invocations_rank_index desc
    {% endset %}

    {% set valid_ids_query %}
        select distinct id
        from ({{ ordered_test_results_query }}) as ordered_test_results
        where invocations_rank_index = 1
    {% endset %}

    {% set test_results_agate = elementary.run_query(test_results_agate_sql) %}
    {% if not skip_test_result_rows %}
        {% set test_result_rows_agate = elementary_cli.get_result_rows_agate(days_back, valid_ids_query) %}
    {% else %}
        {% set test_result_rows_agate = {} %}
    {% endif %}

    {% do return(elementary_cli._process_raw_test_results(test_results_agate, test_result_rows_agate, elementary_tests_allowlist_status, skip_test_result_rows)) %}
{%- endmacro -%}
