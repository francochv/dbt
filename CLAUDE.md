# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## Project Overview

**NexaRetail Analytics Platform** — pipeline de transformación de datos para una empresa ficticia de retail omnicanal. Integra datos de Shopify (e-commerce), Stripe (pagos) y un ERP en SQL Server para producir modelos analíticos dimensionales sobre una arquitectura Lakehouse (Bronze → Silver → Gold).

Motor de cómputo: **DuckDB**. Almacenamiento: **Parquet / Delta Lake** sobre OneLake (Azure). Orquestación: **Dagster**.

---

## dbt Commands

```bash
# Desarrollo
dbt run                                   # Ejecutar todos los modelos
dbt run --select staging                  # Solo capa staging
dbt run --select +fct_orders              # fct_orders y upstream
dbt run --select fct_orders+              # fct_orders y downstream
dbt run --full-refresh                    # Reconstruir incrementales

# Testing
dbt test                                  # Todos los tests
dbt test --select stg_stripe__payments    # Tests de un modelo específico
dbt build                                 # run + test en orden DAG

# Calidad de fuentes
dbt source freshness                      # Validar frescura de fuentes

# Documentación
dbt docs generate && dbt docs serve

# Debug
dbt compile --select <model>              # Compilar SQL sin ejecutar
dbt debug                                 # Verificar conexión
dbt ls --select tag:critical              # Listar modelos por tag

# Dev scope (usa macro limit_data_in_dev)
dbt run --target dev                      # Solo últimos 3 días de datos
```

---

## Architecture: Medallion Lakehouse

```
Sources (raw / Bronze)
    Shopify via Fivetran → raw.shopify.*
    Stripe via Fivetran  → raw.stripe.*
    SQL Server ERP       → raw.erp.*  (CDC / batch diario)
    APIs externas        → raw.api.*  (archivos Parquet en OneLake)
         ↓
Staging (Silver) — views, 1:1 con source, solo limpieza
         ↓
Intermediate — lógica de negocio, joins, agregaciones (ephemeral)
         ↓
Marts (Gold) — dimensiones y hechos materializados como tables
```

### Materialización por capa (`dbt_project.yml`)

| Capa         | Materialización | Schema              |
|--------------|-----------------|---------------------|
| staging      | view            | `<target>_staging`  |
| intermediate | ephemeral       | (no schema propio)  |
| marts        | table           | `<target>_analytics`|

El macro `generate_schema_name` concatena el schema del target con el schema personalizado (`<target>_<custom_schema>`), evitando colisiones entre entornos dev/prod.

---

## Model Naming Conventions

| Capa         | Prefijo        | Ejemplo                            |
|--------------|----------------|------------------------------------|
| Staging      | `stg_`         | `stg_stripe__payments`             |
| Intermediate | `int_`         | `int_payments_pivoted_to_customer` |
| Dimension    | `dim_`         | `dim_customers`                    |
| Fact         | `fct_`         | `fct_orders`                       |

Fuente doble guión bajo: `stg_<source>__<entity>` (e.g., `stg_shopify__orders`).

---

## Incremental Strategies

Los modelos incrementales siguen esta lógica según el caso de uso:

```sql
-- Pagos / eventos: delete+insert por unique_key
config(materialized='incremental', unique_key='payment_id',
       incremental_strategy='delete+insert')

-- Órdenes con late-arriving updates: merge selectivo
config(materialized='incremental', unique_key='order_id',
       incremental_strategy='merge',
       merge_update_columns=['status', 'total_price', 'updated_at'])

-- Eventos de alto volumen: insert_overwrite por partición de fecha
config(materialized='incremental',
       incremental_strategy='insert_overwrite',
       partition_by={"field": "event_date", "data_type": "date", "granularity": "day"})
```

Filtro incremental estándar en staging de pagos:
```sql
{% if is_incremental() %}
where _fivetran_synced > (select max(_loaded_at) from {{ this }})
{% endif %}
```

---

## Testing Requirements

Todo modelo de marts debe tener en su `.yml`:
- `unique` + `not_null` en surrogate key y natural key
- `relationships` en todas las foreign keys apuntando a dimensiones
- `accepted_values` en columnas de categoría/status
- `dbt_utils.expression_is_true` para métricas numéricas (ej. `>= 0`)
- `dbt_utils.recency` en tablas de hechos para alertar sobre datos viejos

Fuentes: configurar `freshness` con `warn_after: 12h` y `error_after: 24h`.

---

## Key Macros

| Macro                  | Uso                                                              |
|------------------------|------------------------------------------------------------------|
| `cents_to_dollars`     | Convierte centavos: `{{ cents_to_dollars('amount') }}`          |
| `generate_schema_name` | Concatena target + custom schema para aislar entornos           |
| `limit_data_in_dev`    | Filtra últimos N días en target `dev` para acelerar desarrollo  |

```sql
-- Uso de limit_data_in_dev al final de un modelo staging
select * from {{ source('shopify', 'orders') }}
{{ limit_data_in_dev('created_at', days=3) }}
```

---

## Star Schema (Marts Core)

```
dim_customers ──┐
                ├── fct_orders
                └── (futuro) fct_revenue
```

`dim_customers` se construye desde `int_payments_pivoted_to_customer` (métricas Stripe) cruzado con resumen de órdenes Shopify. Incluye surrogate key generada con `dbt_utils.generate_surrogate_key`.

`fct_orders` referencia `dim_customers` vía `customer_key` (surrogate key), no por `customer_id` natural.

---

## Variables del Proyecto

```yaml
vars:
  start_date: "2020-01-01"   # Fecha mínima para full-refresh
```

Usar `{{ var('start_date') }}` en lugar de fechas hardcodeadas en modelos.

---

## Source Freshness Pattern

Todas las fuentes Fivetran usan `loaded_at_field: _fivetran_synced`. Las fuentes ERP/API deben definir su propio campo de timestamp de carga y configurar freshness equivalente.

---

## DuckDB / Parquet Context

Este proyecto está diseñado para ejecutarse sobre DuckDB como motor central:
- Los archivos Parquet en OneLake se leen directamente via DuckDB con `read_parquet()`
- Las particiones por fecha (`event_date`, `created_date`) habilitan partition pruning
- Para volúmenes > 1M filas, preferir `insert_overwrite` por partición sobre merge completo
- Formatos de salida: Parquet comprimido con Snappy para Gold, Delta para tablas con CDC

