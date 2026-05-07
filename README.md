# NexaRetail Analytics Platform

Pipeline de transformación de datos para un retailer omnicanal ficticio construido sobre una arquitectura **Medallion Lakehouse** (Bronze → Silver → Gold) usando **dbt + DuckDB**.

> Proyecto didáctico diseñado para demostrar patrones de ingeniería de datos modernos: modelado dimensional, estrategias incrementales, calidad de datos y orquestación.

---

## Tabla de contenidos

1. [¿Qué hace este proyecto?](#1-qué-hace-este-proyecto)
2. [Arquitectura](#2-arquitectura)
3. [Stack tecnológico](#3-stack-tecnológico)
4. [Requisitos previos](#4-requisitos-previos)
5. [Instalación paso a paso](#5-instalación-paso-a-paso)
6. [Configurar el perfil de DuckDB](#6-configurar-el-perfil-de-duckdb)
7. [Estructura del proyecto](#7-estructura-del-proyecto)
8. [Ejecutar el pipeline completo](#8-ejecutar-el-pipeline-completo)
9. [Fases del proyecto explicadas](#9-fases-del-proyecto-explicadas)
10. [Tests y calidad de datos](#10-tests-y-calidad-de-datos)
11. [Estrategias incrementales](#11-estrategias-incrementales)
12. [Snapshots (SCD Tipo 2)](#12-snapshots-scd-tipo-2)
13. [Comandos útiles de referencia](#13-comandos-útiles-de-referencia)
14. [Cómo contribuir y sincronizar con GitHub](#14-cómo-contribuir-y-sincronizar-con-github)

---

## 1. ¿Qué hace este proyecto?

NexaRetail es una empresa ficticia de retail omnicanal que vende productos a través de su tienda online (Shopify), procesa pagos con Stripe y gestiona inventario y proveedores mediante un ERP en SQL Server.

Este proyecto toma los datos crudos de esas tres fuentes y los transforma en un **star schema analítico** listo para BI:

```
Shopify  ──┐
Stripe   ──┼──▶  dbt (DuckDB)  ──▶  dim_customers / dim_products / dim_date
ERP      ──┘                    ──▶  fct_orders / fct_payments / fct_inventory_snapshots
```

**Casos de uso que habilita:**
- Revenue diario / mensual por canal y segmento de cliente
- Análisis de cohortes de clientes (lifetime value, churn)
- Alertas de stock bajo y ruptura de inventario
- Rentabilidad por producto (margen bruto)
- Evolución histórica de precios y costos (SCD Type 2)

---

## 2. Arquitectura

El proyecto implementa la **arquitectura Medallion** en tres capas:

```
┌─────────────────────────────────────────────────────────────┐
│  BRONZE  (Sources / Raw)                                     │
│  raw.shopify.*   raw.stripe.*   raw.erp.*                   │
│  Datos crudos sin transformar, cargados por Fivetran / CDC  │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│  SILVER  (Staging — views)                                   │
│  Limpieza 1:1 con la fuente: renombrar columnas,            │
│  castear tipos, normalizar texto, estandarizar monedas      │
└────────────────────────┬────────────────────────────────────┘
                         │
                 [Intermediate — ephemeral]
                  Lógica de negocio, joins,
                  agregaciones por dominio
                         │
┌────────────────────────▼────────────────────────────────────┐
│  GOLD  (Marts — tables)                                      │
│  Star schema: dimensiones + tablas de hechos                │
│  Optimizadas para consultas analíticas en DuckDB / Power BI │
└─────────────────────────────────────────────────────────────┘
```

### Star schema resultante

```
         dim_date
            │
dim_products │  dim_customers
     │       │       │
     └───────▼───────┘
         fct_orders
         fct_payments
     fct_inventory_snapshots ──▶ dim_products
```

---

## 3. Stack tecnológico

| Componente | Herramienta | Rol |
|---|---|---|
| Transformación | **dbt Core** | Modelado SQL, tests, documentación |
| Motor de cómputo | **DuckDB** | Procesa Parquet en memoria, SQL analítico |
| Almacenamiento | **Parquet / Delta Lake** | Formato columnar sobre OneLake (Azure) |
| Orquestación | **Dagster** *(referencia)* | DAG scheduling, retries, observabilidad |
| Ingesta | **Fivetran** *(referencia)* | Shopify + Stripe → Bronze |
| CDC | **SQL Server CDC** *(referencia)* | ERP → Bronze batch diario |
| BI | **Power BI** *(referencia)* | Consume la capa Gold |

> **Nota:** Para ejecutar este proyecto localmente solo necesitas Python, dbt-duckdb y DuckDB. El resto del stack es contextual al entorno de producción.

---

## 4. Requisitos previos

Antes de empezar, asegúrate de tener instalado:

| Requisito | Versión mínima | Verificar |
|---|---|---|
| Python | 3.9+ | `python --version` |
| pip | 23+ | `pip --version` |
| git | 2.x | `git --version` |
| dbt-core | 1.7+ | `dbt --version` |
| dbt-duckdb | 1.7+ | `dbt --version` |

---

## 5. Instalación paso a paso

### Paso 1 — Clonar el repositorio

```bash
git clone https://github.com/tu-usuario/nexaretail-analytics.git
cd nexaretail-analytics
```

### Paso 2 — Crear entorno virtual e instalar dependencias

```bash
# Crear entorno virtual
python -m venv .venv

# Activar (Linux/Mac)
source .venv/bin/activate

# Activar (Windows PowerShell)
.venv\Scripts\Activate.ps1

# Instalar dbt con el adaptador de DuckDB
pip install dbt-duckdb
```

### Paso 3 — Instalar paquetes dbt

```bash
dbt deps
```

Esto instala `dbt_utils` declarado en [packages.yml](packages.yml), que provee macros como `generate_surrogate_key`, `date_spine` y `expression_is_true`.

### Paso 4 — Verificar la instalación

```bash
dbt debug
```

Si todo está bien verás `All checks passed!`. Si falla, revisa el paso siguiente.

---

## 6. Configurar el perfil de DuckDB

dbt necesita un perfil de conexión en `~/.dbt/profiles.yml` (fuera del repositorio, por seguridad).

### Opción A — DuckDB en archivo local (recomendado para desarrollo)

Crea o edita el archivo `~/.dbt/profiles.yml`:

```yaml
analytics:
  target: dev
  outputs:

    dev:
      type: duckdb
      path: "{{ env_var('DBT_DUCKDB_PATH', 'dev.duckdb') }}"
      threads: 4

    prod:
      type: duckdb
      path: "{{ env_var('DBT_DUCKDB_PATH', 'prod.duckdb') }}"
      threads: 8
```

### Opción B — DuckDB en memoria (descartable, para CI/CD)

```yaml
analytics:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ":memory:"
      threads: 4
```

### Opción C — DuckDB leyendo Parquet desde OneLake (producción)

```yaml
analytics:
  target: prod
  outputs:
    prod:
      type: duckdb
      path: "prod.duckdb"
      threads: 8
      extensions:
        - azure
      settings:
        azure_storage_connection_string: "{{ env_var('AZURE_STORAGE_CONNECTION_STRING') }}"
```

> **¿Por qué DuckDB?** A diferencia de un warehouse tradicional, DuckDB lee archivos Parquet directamente con `read_parquet()` sin necesidad de cargar los datos. Esto lo convierte en el motor ideal para arquitecturas Lakehouse donde el almacenamiento es barato y el cómputo es efímero.

---

## 7. Estructura del proyecto

```
nexaretail-analytics/
│
├── dbt_project.yml              # Config central: capas, schemas, tags
├── packages.yml                 # Dependencias dbt (dbt_utils)
│
├── models/
│   ├── staging/                 # Silver — views 1:1 con fuente
│   │   ├── stripe/
│   │   │   ├── _stripe__sources.yml     # Definición de fuentes + freshness
│   │   │   ├── _stripe__models.yml      # Documentación + tests de staging
│   │   │   ├── stg_stripe__customers.sql
│   │   │   ├── stg_stripe__payments.sql  ← incremental (delete+insert)
│   │   │   └── stg_stripe__invoices.sql  ← incremental (delete+insert)
│   │   ├── shopify/
│   │   │   ├── _shopify__sources.yml
│   │   │   ├── _shopify__models.yml
│   │   │   ├── stg_shopify__orders.sql
│   │   │   ├── stg_shopify__customers.sql
│   │   │   ├── stg_shopify__products.sql
│   │   │   └── stg_shopify__order_items.sql
│   │   └── erp/
│   │       ├── _erp__sources.yml
│   │       ├── _erp__models.yml
│   │       ├── stg_erp__products.sql
│   │       ├── stg_erp__inventory.sql    ← incremental (insert_overwrite por partición)
│   │       └── stg_erp__suppliers.sql
│   │
│   ├── intermediate/            # Ephemeral — lógica de negocio
│   │   ├── customers/
│   │   │   └── int_customer_orders_summary.sql
│   │   ├── finance/
│   │   │   ├── int_payments_pivoted_to_customer.sql
│   │   │   └── int_orders_revenue_daily.sql
│   │   ├── orders/
│   │   │   └── int_orders_with_items.sql
│   │   └── supply_chain/
│   │       └── int_inventory_stock_levels.sql
│   │
│   └── marts/                   # Gold — tables materializadas
│       ├── core/
│       │   ├── _core__models.yml        # Tests completos de marts
│       │   ├── dim_date.sql
│       │   ├── dim_customers.sql
│       │   ├── dim_products.sql
│       │   ├── fct_orders.sql            ← incremental (merge)
│       │   └── fct_payments.sql          ← incremental (delete+insert)
│       └── supply_chain/
│           ├── _supply_chain__models.yml
│           └── fct_inventory_snapshots.sql ← incremental (insert_overwrite)
│
├── snapshots/
│   ├── scd_customers.sql        # SCD Type 2 — historial de cambios en clientes
│   └── scd_products.sql         # SCD Type 2 — historial de cambios en precios/costos
│
├── seeds/
│   ├── ref_order_status.csv     # Tabla de referencia: estados de órdenes
│   └── ref_payment_status.csv   # Tabla de referencia: estados de pagos
│
└── macros/
    ├── cents_to_dollars.sql     # Convierte centavos: amount / 100.0
    ├── generate_schema_name.sql # Aisla entornos dev/prod
    └── limit_data_in_dev.sql    # Filtra últimos N días en target=dev
```

---

## 8. Ejecutar el pipeline completo

Sigue estos pasos en orden la primera vez:

### Paso 1 — Cargar tablas de referencia (seeds)

```bash
dbt seed
```

Carga `ref_order_status` y `ref_payment_status` como tablas en DuckDB. Estos son datos estáticos que no cambian frecuentemente.

### Paso 2 — Full refresh inicial (primera ejecución)

```bash
dbt build --full-refresh
```

`dbt build` ejecuta en orden DAG: seeds → run → test → snapshots. El flag `--full-refresh` reconstruye todos los modelos incrementales desde cero.

> **¿Cuándo usar `--full-refresh`?**
> - Primera vez que corres el proyecto
> - Cuando cambias la lógica de un modelo incremental
> - Cuando hay un backfill masivo de datos históricos
> - Cuando cambias el `unique_key` de un incremental

### Paso 3 — Ejecuciones diarias (incremental)

```bash
dbt build
```

En las ejecuciones subsecuentes, los modelos incrementales solo procesan los registros nuevos/modificados, haciendo el pipeline mucho más eficiente.

### Paso 4 — Actualizar snapshots

```bash
dbt snapshot
```

Los snapshots se ejecutan separadamente de `dbt build`. Deben correr después de que el staging esté actualizado.

---

## 9. Fases del proyecto explicadas

### Fase 1 — Bronze (Sources)

Los datos crudos viven en el schema `raw` de DuckDB. Las fuentes están declaradas en archivos `_*__sources.yml` con configuración de **freshness** para alertar cuando los datos no se actualizan:

```yaml
freshness:
  warn_after:  { count: 12, period: hour }   # Alerta si datos > 12h
  error_after: { count: 24, period: hour }   # Error si datos > 24h
```

Para verificar la frescura de las fuentes:

```bash
dbt source freshness
```

**Fuentes disponibles:**

| Fuente | Schema raw | Loader | Tablas |
|---|---|---|---|
| Stripe | `raw.stripe` | Fivetran | customers, payments, invoices |
| Shopify | `raw.shopify` | Fivetran | orders, customers, products, order_line |
| ERP SQL Server | `raw.erp` | CDC batch | products, inventory_snapshots, suppliers |

---

### Fase 2 — Silver (Staging)

Los modelos staging hacen **solo** limpieza básica: renombrar columnas a snake_case, castear tipos, normalizar texto y convertir monedas. **No hay lógica de negocio aquí.**

Convención de naming: `stg_<fuente>__<entidad>`

**Ejemplo — Conversión de centavos a dólares:**

```sql
-- En lugar de:  amount / 100.0 as amount
-- Se usa la macro:
{{ cents_to_dollars('amount') }} as amount
```

**Ejemplo — Limitar datos en desarrollo:**

```sql
-- Al final de un modelo staging, esto añade un WHERE solo en target=dev:
select * from {{ source('shopify', 'orders') }}
{{ limit_data_in_dev('created_at', days=3) }}
```

Esto evita procesar años de histórico en cada `dbt run` durante el desarrollo.

---

### Fase 3 — Intermediate (Lógica de negocio)

Los modelos intermediate son **ephemeral**: no crean tablas físicas, solo son CTEs reutilizables que se inyectan en los modelos downstream. Esto minimiza el almacenamiento sin sacrificar legibilidad.

| Modelo | Qué hace |
|---|---|
| `int_orders_with_items` | Join órdenes + líneas, suma totales por orden |
| `int_customer_orders_summary` | KPIs de cliente desde Shopify (órdenes, revenue, LTV) |
| `int_payments_pivoted_to_customer` | Agrega métricas de pago Stripe por cliente |
| `int_inventory_stock_levels` | Último snapshot por producto+almacén con `QUALIFY` |
| `int_orders_revenue_daily` | Agrega revenue diario para análisis de tendencias |

---

### Fase 4 — Gold (Marts)

Los modelos Gold son tablas físicas optimizadas para consultas analíticas. Siguen un **star schema** donde las dimensiones (`dim_*`) conectan con las tablas de hechos (`fct_*`) a través de surrogate keys.

#### ¿Por qué surrogate keys?

En lugar de usar el `customer_id` natural del sistema fuente como FK, usamos una **surrogate key** generada con `dbt_utils.generate_surrogate_key`:

```sql
{{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key
```

**Ventajas:**
- Independencia del sistema fuente (si el ID natural cambia, la SK no)
- Consistencia entre sistemas (el mismo cliente en Stripe y Shopify tiene la misma SK)
- Compatible con SCD Type 2 (cada versión histórica tiene una SK única)

#### dim_date — ¿Por qué una tabla de fechas?

```sql
-- En lugar de hacer date_trunc en cada query analítica:
select year, month, is_weekend from dim_date where date_day = '2024-03-15'
```

La dimensión de fechas elimina cálculos repetitivos y permite filtros rápidos por atributos de tiempo (año fiscal, semana, día hábil) sin transformaciones en cada consulta.

---

## 10. Tests y calidad de datos

Cada modelo tiene tests declarados en su `.yml`. dbt ejecuta estos tests como queries SQL y falla si encuentran registros inválidos.

### Tipos de tests implementados

| Test | Qué valida | Ejemplo |
|---|---|---|
| `unique` | No hay duplicados en la columna | `customer_key` debe ser único |
| `not_null` | No hay valores nulos | `order_id` nunca puede ser NULL |
| `accepted_values` | Solo valores permitidos | `order_status` ∈ {open, closed, cancelled, archived} |
| `relationships` | FK existe en la tabla referenciada | `customer_key` en `fct_orders` → `dim_customers` |
| `dbt_utils.expression_is_true` | Expresión SQL es verdadera | `amount >= 0` (no hay montos negativos) |
| `dbt_utils.recency` | Tabla tiene datos recientes | `fct_orders` tiene registros del último día |

### Ejecutar tests

```bash
# Todos los tests
dbt test

# Solo tests de un modelo específico
dbt test --select dim_customers

# Solo tests de una capa
dbt test --select tag:critical

# Ver los registros que fallan (útil para debug)
dbt test --store-failures
```

### Tests en fuentes (source freshness)

```bash
dbt source freshness
```

Consulta el campo `_fivetran_synced` (o `_batch_loaded_at` para ERP) y alerta si los datos tienen más de 12h de antigüedad.

---

## 11. Estrategias incrementales

Los modelos de alto volumen usan estrategias incrementales para procesar solo los datos nuevos en cada ejecución.

### delete+insert — Pagos y eventos

Ideal cuando los registros no se modifican después de crearse:

```sql
-- stg_stripe__payments.sql
config(materialized='incremental', unique_key='payment_id',
       incremental_strategy='delete+insert')

{% if is_incremental() %}
where _fivetran_synced > (select max(_loaded_at) from {{ this }})
{% endif %}
```

**Flujo:** Elimina los registros que ya existen para las IDs del nuevo batch, luego inserta todos los del batch. Garantiza idempotencia.

### merge — Órdenes con late-arriving updates

Ideal cuando los registros pueden actualizarse después de crearse (e.g., una orden cambia de `open` a `closed`):

```sql
-- fct_orders.sql
config(materialized='incremental', unique_key='order_id',
       incremental_strategy='merge',
       merge_update_columns=['order_status', 'total_price', 'updated_at'])
```

**Ventaja de `merge_update_columns`:** Solo actualiza las columnas declaradas, evitando sobrescribir métricas históricas.

### insert_overwrite — Inventario por partición de fecha

Ideal para snapshots diarios de alto volumen donde reemplazas particiones enteras:

```sql
-- stg_erp__inventory.sql
config(materialized='incremental',
       incremental_strategy='insert_overwrite',
       partition_by={"field": "snapshot_date", "data_type": "date", "granularity": "day"})

{% if is_incremental() %}
where snapshot_date >= current_date - INTERVAL 3 DAY
{% endif %}
```

**Por qué 3 días y no 1?** Para absorber recargas tardías del ERP. Si el batch de ayer llegó con retraso, los 3 días garantizan que se reprocese correctamente.

---

## 12. Snapshots (SCD Tipo 2)

Los snapshots rastrean cambios históricos en dimensiones que mutan con el tiempo. dbt añade automáticamente columnas `dbt_valid_from` y `dbt_valid_to`.

```bash
dbt snapshot
```

**Ejemplo de uso:** Si el precio de un producto cambia de $50 a $60:

```
product_code | list_price | dbt_valid_from | dbt_valid_to
PROD-001     | 50.00      | 2024-01-01     | 2024-06-15
PROD-001     | 60.00      | 2024-06-15     | null          ← versión actual
```

**Snapshots disponibles:**

| Snapshot | Qué rastrea |
|---|---|
| `scd_customers` | Cambios de email, nombre, país |
| `scd_products` | Cambios de precio, costo, estado |

---

## 13. Comandos útiles de referencia

```bash
# ── Desarrollo ──────────────────────────────────────────────
dbt run                                   # Correr todos los modelos
dbt run --select staging                  # Solo capa staging
dbt run --select +fct_orders              # fct_orders y todos sus upstream
dbt run --select fct_orders+              # fct_orders y todos sus downstream
dbt run --select tag:critical             # Solo modelos con tag critical
dbt run --full-refresh                    # Reconstruir incrementales desde cero
dbt run --target dev                      # Usar target dev (solo últimos 3 días)

# ── Testing ──────────────────────────────────────────────────
dbt test                                  # Todos los tests
dbt test --select stg_stripe__payments    # Tests de un modelo específico
dbt test --store-failures                 # Guarda registros fallidos en tabla

# ── Build (run + test en orden DAG) ──────────────────────────
dbt build                                 # Pipeline completo incremental
dbt build --full-refresh                  # Pipeline completo desde cero
dbt build --select +dim_customers         # dim_customers y su upstream

# ── Fuentes ──────────────────────────────────────────────────
dbt source freshness                      # Validar frescura de datos

# ── Snapshots ────────────────────────────────────────────────
dbt snapshot                              # Ejecutar todos los snapshots

# ── Seeds ────────────────────────────────────────────────────
dbt seed                                  # Cargar tablas de referencia

# ── Documentación ────────────────────────────────────────────
dbt docs generate                         # Generar documentación
dbt docs serve                            # Servir docs en http://localhost:8080

# ── Debug ────────────────────────────────────────────────────
dbt compile --select fct_orders           # Compilar SQL sin ejecutar
dbt debug                                 # Verificar conexión y config
dbt ls --select tag:critical              # Listar modelos por tag
dbt ls --select +fct_orders               # Listar upstream de un modelo
```

---

## 14. Cómo contribuir y sincronizar con GitHub

### Configuración inicial del repositorio

Si estás clonando este proyecto por primera vez:

```bash
# 1. Clonar
git clone https://github.com/tu-usuario/nexaretail-analytics.git
cd nexaretail-analytics

# 2. Instalar dependencias
python -m venv .venv
source .venv/bin/activate          # o .venv\Scripts\Activate.ps1 en Windows
pip install dbt-duckdb
dbt deps

# 3. Configurar perfil (ver Sección 6)
# Crear ~/.dbt/profiles.yml con tu configuración

# 4. Verificar
dbt debug
```

### Flujo de trabajo para contribuir

```bash
# 1. Crear rama para tu cambio
git checkout -b feat/nuevo-modelo-fct-returns

# 2. Desarrollar el modelo
#    - Crear el .sql en la capa correcta
#    - Añadir documentación y tests en el .yml correspondiente

# 3. Verificar que el modelo compila
dbt compile --select tu_nuevo_modelo

# 4. Correr y testear el modelo con sus upstream
dbt build --select +tu_nuevo_modelo

# 5. Hacer commit
git add models/marts/core/fct_returns.sql
git add models/marts/core/_core__models.yml
git commit -m "feat: add fct_returns model for refund analysis"

# 6. Push y Pull Request
git push origin feat/nuevo-modelo-fct-returns
```

### Archivos que NO deben subirse a GitHub

Asegúrate de que tu `.gitignore` incluya:

```gitignore
# Bases de datos DuckDB locales
*.duckdb
*.duckdb.wal

# Artefactos de dbt
target/
dbt_packages/
logs/

# Entorno virtual de Python
.venv/
__pycache__/

# Credenciales (NUNCA subir)
.env
profiles.yml
```

> **Importante:** El archivo `~/.dbt/profiles.yml` contiene credenciales y **nunca debe estar en el repositorio**. Cada persona que clone el proyecto debe configurar el suyo localmente.

### Checklist antes de hacer PR

- [ ] `dbt compile --select <modelo>` no tiene errores de sintaxis
- [ ] `dbt build --select +<modelo>` pasa sin errores
- [ ] El modelo tiene documentación (`description`) en su `.yml`
- [ ] Las columnas clave tienen tests (`unique`, `not_null`)
- [ ] Si es un mart: tiene `relationships` en todas las FKs
- [ ] Si es incremental: tiene el bloque `{% if is_incremental() %}`
- [ ] No hay fechas hardcodeadas (usar `{{ var('start_date') }}`)
- [ ] No hay credenciales ni rutas locales absolutas en el código

---

## Macros disponibles

| Macro | Sintaxis | Descripción |
|---|---|---|
| `cents_to_dollars` | `{{ cents_to_dollars('amount_cents') }}` | Divide por 100.0 con redondeo |
| `generate_schema_name` | *(automático)* | Genera `<target>_<custom_schema>` para aislar dev/prod |
| `limit_data_in_dev` | `{{ limit_data_in_dev('created_at', days=3) }}` | WHERE solo en target=dev, vacío en prod |

---

## Variables del proyecto

Definidas en `dbt_project.yml`:

| Variable | Valor por defecto | Uso |
|---|---|---|
| `start_date` | `"2020-01-01"` | Fecha mínima para full-refresh y date_spine |

Sobreescribir en ejecución:

```bash
dbt run --vars '{"start_date": "2023-01-01"}'
```

---

*Proyecto construido con [dbt](https://www.getdbt.com/) · Motor: [DuckDB](https://duckdb.org/) · Arquitectura: Medallion Lakehouse*
