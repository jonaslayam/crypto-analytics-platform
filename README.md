# 📊 Quantitative Market Intelligence ELT Pipeline
**End-to-End Data Engineering Portfolio: From API Extraction to BI Insights**

*(Lee la versión en español más abajo / Spanish version below)*

---

## 🇺🇸 Project Overview (English)
This project is an end-to-end market intelligence solution that automates the extraction, transformation, and visualization of near real-time cryptocurrency data. It implements a robust ELT pipeline to generate trading signals based on statistical (Z-Score) and technical (RSI) indicators.

## 🧭 Architecture Diagram

```text
          ┌────────────────────┐
          │   CoinCap API      │
          └─────────┬──────────┘
                    │
            (Airflow DAGs)
                    │
          ┌─────────▼──────────┐
          │  Python Extraction │
          └─────────┬──────────┘
                    │
          ┌─────────▼──────────┐
          │      DuckDB        │
          │ JSON → Parquet     │
          └─────────┬──────────┘
                    │
          ┌─────────▼──────────┐
          │ OCI Object Storage │
          │ (Hive Partitioned) │
          └─────────┬──────────┘
                    │
          ┌─────────▼──────────┐
          │   Oracle ADW       │
          │ External Tables    │
          └─────────┬──────────┘
                    │
          ┌─────────▼──────────┐
          │        dbt         │
          │  Star Schema       │
          └─────────┬──────────┘
                    │
          ┌─────────▼──────────┐
          │     Power BI       │
          │ Trading Signals    │
          └────────────────────┘
```

### 🚀 Data Engineering Stack
* **Infrastructure & Orchestration:** Kubernetes (k3d), Apache Airflow, Docker.
* **Data Ingestion:** Python, CoinCap REST API.
* **Data Lake Processing:** DuckDB (Efficient JSON-to-Parquet processing with in-memory and disk-based execution).
* **Storage:** OCI Object Storage (Hive-partitioned Data Lake).
* **Data Warehouse & Transformation:** Oracle Autonomous Data Warehouse (ADW) via External Tables, dbt (Data Build Tool), SQL.
* **Analytics / BI:** Power BI (Advanced DAX for signal generation).

## ⚙️ Why This Stack?

* **DuckDB vs Other Solutions:** DuckDB was selected for its lightweight architecture and its ability to perform analytical processing directly on object storage (OCI Object Storage) without requiring a distributed cluster or intermediate layers. In this pipeline, DuckDB reads JSON data directly from Object Storage using `read_json_auto`, applies SQL-based transformations (including window functions), and writes the results back to the data lake in Parquet format.

    Its vectorized execution engine and optimized columnar processing, combined with out-of-memory execution capabilities, enable efficient scaling as data volume and the number of tracked assets grow. This approach eliminates the need for local staging or additional data pipelines, reducing latency, operational complexity, and infrastructure costs.
* **External Tables (ADW):** Enables zero-copy querying directly over Parquet files stored in Object Storage, eliminating data duplication and reducing both storage and ingestion costs while maintaining high query performance through predicate pushdown and partition pruning.
* **dbt for Transformation:** Provides modular, testable, and version-controlled SQL transformations, aligning with modern analytics engineering practices.
* **Airflow + Kubernetes:** Ensures scalability, fault tolerance, and production-grade orchestration of the entire pipeline.

### ⚖️ Trade-offs

- This architecture prioritizes simplicity and cost-efficiency over fully distributed processing (e.g., Spark), making it ideal for mid-scale workloads but not designed for petabyte-scale real-time processing.

### 🏗️ Pipeline Architecture (Lakehouse Approach)
1. **Extraction (Raw Layer):** Python scripts running in Airflow pods fetch market data from the CoinCap API and store it as raw JSON.
2. **Processing & Data Lake (Bronze/Silver):** DuckDB engines process the raw JSON payloads, compressing them into highly efficient columnar Parquet files. These files are stored in OCI Object Storage utilizing a strict Hive-style partitioning scheme (`year=.../month=.../day=...`) to optimize query scanning.
3. **Data Warehouse Integration:** Oracle ADW mounts the Hive-partitioned Parquet files as External Tables, enabling zero-copy data querying directly from the Object Storage.
4. **Transformation (Gold Layer):** dbt connects to ADW to clean, cast, and aggregate the external data into a highly performant Star Schema (Fact and Dimension tables).
5. **Serving:** Power BI connects directly to the ADW dimensional models to calculate near real-time trading indicators (Z-Score, RSI, Moving Averages).

### 💡 Business Value (Trading Signals)
The downstream dashboard automatically calculates quantitative trading signals, enabling faster identification of market opportunities by reducing manual analysis and highlighting statistically significant price movements in near real-time.
* **Confirmed Buy:** Moving Average crossover + Low RSI.
* **Take Profit:** Extreme Z-Score peaks (statistical anomalies) and Overbought RSI.

## 🧠 Engineering Challenge

During execution, the pipeline encountered intermittent failures in dbt compilation due to file descriptor limits inside Kubernetes pods:

Error:
`inotify watcher: too many open files`

### Impact
- Silent dbt compilation failures
- Pipeline instability in production environments

### Solution
- Implemented a mandatory `dbt clean` step before execution
- Removed cached artifacts (`target/`, `dbt_packages/`)
- Ensured file usage remained below system limits (`ulimit -n`)

### Result
- Stable and predictable dbt runs
- Elimination of silent failures in orchestration

### 🔮 Future Enhancements (Roadmap)
While the current v1.0 pipeline successfully delivers batch-based market intelligence, the architecture is designed to accommodate the following future iterations:

* **Predictive Machine Learning (ML):** Evolve from purely statistical/technical indicators (Z-Score, RSI) to predictive modeling using **Oracle Machine Learning (OML)**, allowing the dashboard to forecast trend reversals before they happen.
* **Data Observability & Lineage:** Integrate **OpenLineage** or **DataHub** to provide end-to-end visual tracking of data transformations, ensuring strict data governance from the raw JSON payload down to the final Power BI DAX measures.
* **Event-Driven Streaming Transition:** Upgrade the current micro-batch polling architecture (Airflow scheduling) to a pure near real-time streaming approach (using **WebSockets** and **Redpanda/Kafka**) to reduce signal generation latency from minutes to milliseconds.
* **Infrastructure as Code (IaC) & CI/CD:** Implement **Terraform** to automate the provisioning of Kubernetes and OCI Object Storage resources, paired with GitHub Actions for automated `dbt test` execution on every pull request.
* **Automated Unit Testing (Pytest):** Introduce `pytest` to validate the custom Python extraction logic and Airflow DAG integrity prior to deployment. This complements `dbt test` (which handles data quality) by ensuring the ELT code is robust and gracefully handles API rate limits or malformed JSON payloads via mocking.

## 🧪 Key Learnings

* Designing cost-efficient lakehouse architectures without relying on heavy distributed systems
* Handling real-world orchestration issues in Kubernetes environments
* Building scalable ELT pipelines with minimal infrastructure overhead
* Eyeballing a handful of recent signals on the dashboard felt promising, but mechanically replaying all 122 of them across a full year (see the backtest above) told a different story. Manual spot-checks are biased toward whatever caught your attention recently; a backtest forces you to confront every signal the rules actually generate, including the ones during the bad stretches you'd otherwise skip past

---

## 🇪🇸 Resumen del Proyecto (Spanish)
Este proyecto es una solución integral de inteligencia de mercado que automatiza la extracción, transformación y visualización de datos de criptomonedas en tiempo casi real. Implementa un pipeline robusto de datos (ELT) para generar señales de trading basadas en indicadores estadísticos (Z-Score) y técnicos (RSI).

## 🧭 Diagrama de Arquitectura

```text
          ┌────────────────────┐
          │   API CoinCap      │
          └─────────┬──────────┘
                    │
            (DAGs de Airflow)
                    │
          ┌─────────▼──────────┐
          │ Extracción Python  │
          └─────────┬──────────┘
                    │
          ┌─────────▼──────────┐
          │      DuckDB        │
          │ JSON → Parquet     │
          └─────────┬──────────┘
                    │
          ┌─────────▼──────────┐
          │ OCI Object Storage │
          │ (Particionado Hive)│
          └─────────┬──────────┘
                    │
          ┌─────────▼──────────┐
          │   Oracle ADW       │
          │ Tablas Externas    │
          └─────────┬──────────┘
                    │
          ┌─────────▼──────────┐
          │        dbt         │
          │  Modelo Estrella   │
          └─────────┬──────────┘
                    │
          ┌─────────▼──────────┐
          │     Power BI       │
          │ Señales Trading    │
          └────────────────────┘
```

### 🚀 Stack Tecnológico
* **Infraestructura y Orquestación:** Kubernetes (k3d), Apache Airflow, Docker.
* **Ingesta de Datos:** Python, CoinCap REST API.
* **Procesamiento en Data Lake:** DuckDB (Transformación eficiente de JSON a Parquet utilizando ejecución híbrida en memoria y disco).
* **Almacenamiento:** OCI Object Storage (Data Lake particionado en formato Hive).
* **Data Warehouse y Transformación:** Oracle Autonomous Data Warehouse (ADW) mediante Tablas Externas, dbt (Data Build Tool), SQL.
* **Analítica / BI:** Power BI (DAX Avanzado).

## ⚙️ ¿Por qué este stack?

* **DuckDB vs Otras Alternativas:** Se eligió DuckDB por su arquitectura liviana y su capacidad de ejecutar procesamiento analítico directamente sobre almacenamiento de objetos (OCI Object Storage) sin requerir un clúster distribuido ni capas intermedias. En este pipeline, DuckDB consume datos JSON directamente desde Object Storage utilizando `read_json_auto`, los transforma mediante SQL analítico (incluyendo funciones de ventana) y escribe los resultados en formato Parquet nuevamente en el data lake.

    Su motor de ejecución vectorizado y su procesamiento columnar optimizado, junto con capacidades de ejecución fuera de memoria, permiten escalar de forma eficiente las transformaciones a medida que crece el volumen de datos y la cantidad de activos monitoreados. Este enfoque elimina la necesidad de staging local o pipelines adicionales, reduciendo la latencia, la complejidad operativa y el costo de infraestructura.
* **Tablas Externas (ADW):** Permiten realizar consultas directamente sobre archivos Parquet almacenados en Object Storage sin necesidad de duplicar datos (zero-copy), reduciendo tanto los costos de almacenamiento como de ingestión, mientras mantienen un alto rendimiento en las consultas mediante técnicas como predicate pushdown y partition pruning.
* **dbt para Transformaciones:** Facilita transformaciones SQL modulares, testeables y versionadas, alineadas con buenas prácticas modernas.
* **Airflow + Kubernetes:** Permite escalabilidad, tolerancia a fallos y orquestación a nivel productivo.

### ⚖️ Trade-offs

- Esta arquitectura prioriza simplicidad y eficiencia de costos por sobre procesamiento distribuido completo (como Spark), siendo ideal para cargas medianas pero no para escenarios de escala masiva en tiempo real.

### 🏗️ Arquitectura del Pipeline (Enfoque Lakehouse)
1. **Extracción (Capa Raw):** Scripts en Python ejecutados en pods de Airflow extraen datos del mercado de la API de CoinCap y los almacenan como JSON crudo.
2. **Procesamiento y Data Lake (Bronze/Silver):** DuckDB procesa los archivos JSON, comprimiéndolos en formato columnar Parquet. Estos archivos se almacenan en OCI Object Storage utilizando un esquema de particionamiento tipo Hive (`year=.../month=.../day=...`) para minimizar el escaneo de datos.
3. **Integración con Data Warehouse:** Oracle ADW monta los archivos Parquet particionados como Tablas Externas, permitiendo consultar los datos directamente desde el Object Storage sin duplicarlos.
4. **Transformación (Capa Gold):** dbt se conecta a ADW para limpiar, tipar y agregar los datos externos, construyendo un Modelo Dimensional (Esquema Estrella) de alto rendimiento.
5. **Consumo:** Power BI se conecta directamente a los modelos dimensionales en ADW para calcular indicadores en tiempo casi real.

### 💡 Business Value (Trading Signals)
El dashboard downstream calcula automáticamente señales de trading cuantitativas, permitiendo una identificación más rápida de oportunidades de mercado al reducir el análisis manual y resaltar movimientos de precio estadísticamente significativos en tiempo casi real.
* **Confirmación de Compra:** Cruce de Media Móvil + RSI bajo.
* **Toma de Ganancia:** Picos extremos de Z-Score (anomalías estadísticas) y RSI sobrecomprado.

## 🧠 Reto de Ingeniería

Durante la ejecución, el pipeline presentó fallos intermitentes en la compilación de dbt debido a límites de archivos abiertos dentro de los pods de Kubernetes:

Error:
`inotify watcher: too many open files`

### Impacto
- Fallos silenciosos en dbt
- Inestabilidad del pipeline en producción

### Solución
- Se implementó un paso obligatorio `dbt clean`
- Eliminación de artefactos cacheados (`target/`, `dbt_packages/`)
- Control del uso de archivos bajo el límite del sistema (`ulimit -n`)

### Resultado
- Ejecuciones estables de dbt
- Eliminación de fallos silenciosos

### 🔮 Futuras Mejoras (Roadmap)
Aunque la versión 1.0 de este pipeline cumple con la generación de inteligencia de mercado por lotes, la arquitectura está preparada para las siguientes evoluciones:

* **Machine Learning Predictivo (ML):** Evolucionar de indicadores puramente estadísticos (Z-Score) a modelos predictivos utilizando **Oracle Machine Learning (OML)** para anticipar cambios de tendencia.
* **Observabilidad y Linaje de Datos:** Integrar **OpenLineage** o **DataHub** para trazar visualmente el ciclo de vida del dato, desde la extracción en la API hasta su consumo en el dashboard, asegurando la gobernanza.
* **Arquitectura de Streaming Orientada a Eventos:** Migrar el actual modelo por lotes (orquestado por Airflow) a una ingesta en tiempo casi real (usando **WebSockets** y **Redpanda/Kafka**) para reducir la latencia de las señales a milisegundos.
* **Infraestructura como Código (IaC) y CI/CD:** Implementar **Terraform** para el despliegue automático de recursos en OCI/Kubernetes, y GitHub Actions para automatizar las pruebas de dbt en cada despliegue.
* **Pruebas Unitarias Automatizadas (Pytest):** Introducir `pytest` para validar los scripts de extracción en Python y la integridad de los DAGs de Airflow antes de su despliegue. Esto complementa a `dbt test` (que asegura la calidad del dato) garantizando que el código ELT sea robusto y maneje correctamente los límites de la API o JSONs malformados mediante *mocking*.

## 🧪 Aprendizajes Clave

* Diseño de arquitecturas lakehouse eficientes en costos sin depender de sistemas distribuidos pesados
* Manejo de problemas reales de orquestación en Kubernetes
* Construcción de pipelines escalables con bajo overhead de infraestructura
* Mirar unas pocas señales recientes en el dashboard daba una impresión positiva, pero replicar mecánicamente las 122 que generan las reglas a lo largo de todo un año (ver el backtest arriba) contó otra historia. El chequeo manual está sesgado hacia lo que llamó la atención recientemente; un backtest obliga a confrontar cada señal que las reglas realmente generan, incluidas las de los tramos malos que uno normalmente pasa por alto

---

## 🛠️ Architecture & Core DAX / Arquitectura y DAX
*Example of the advanced logic implemented to identify market states:*

<details>
<summary><b>Click to view the Core DAX Logic used for Signal Generation</b></summary>

```dax
Current_Market_Status = 
// 1. Find the latest timestamp for the currently evaluated asset
VAR LatestTimestamp = MAX(FCT_CRYPTO_INTRADAY_PRICES[event_time])

// 2. Extract indicators ONLY for that exact latest second
VAR RSIValue = 
    CALCULATE(
        AVERAGE(FCT_CRYPTO_INTRADAY_PRICES[rsi_24h]),
        FCT_CRYPTO_INTRADAY_PRICES[event_time] = LatestTimestamp
    )
VAR ZScoreValue = 
    CALCULATE(
        AVERAGE(FCT_CRYPTO_INTRADAY_PRICES[z_score_24h]),
        FCT_CRYPTO_INTRADAY_PRICES[event_time] = LatestTimestamp
    )
VAR CurrentPrice = 
    CALCULATE(
        AVERAGE(FCT_CRYPTO_INTRADAY_PRICES[price_usd]),
        FCT_CRYPTO_INTRADAY_PRICES[event_time] = LatestTimestamp
    )
VAR MovingAverage = 
    CALCULATE(
        AVERAGE(FCT_CRYPTO_INTRADAY_PRICES[ma_24h_usd]),
        FCT_CRYPTO_INTRADAY_PRICES[event_time] = LatestTimestamp
    )

// 3. Evaluate current market conditions (near real-time)
RETURN
    SWITCH(
        TRUE(),
        ISBLANK(RSIValue), "No Data",
        
        // --- 1. TAKE PROFIT (Exit Signals) ---
        RSIValue >= 70 && ZScoreValue >= 2, "🎯 TAKE PROFIT (Extreme Peak)",
        RSIValue <= 30 && ZScoreValue <= -2, "⚠️ ALERT: Statistical Floor (Low Z-Score)",
        
        // --- 2. DOUBLE CONFIRMATION (Trend Entries) ---
        RSIValue <= 35 && CurrentPrice > MovingAverage, "🚀 CONFIRMED BUY (Bullish Breakout)",
        
        // Acts as a "Stop Loss" (Emergency brake) if there was no extreme peak
        RSIValue >= 65 && CurrentPrice < MovingAverage, "💥 SELL (Trend Reversal)",
        
        // --- 3. WARNING ZONES ---
        RSIValue >= 70, "🟠 Risk: Overbought (Uptrending)",
        RSIValue <= 30, "🟡 Attractive: Oversold (Downtrending)",
        
        "⚪ Neutral"
    )
```

</details>

## 📉 Backtest: Is the Strategy Actually Profitable?

The DAX rules above are fixed technical-indicator thresholds, not a model fit to this data, so they can be tested directly against real history without needing a train/test split. `backtest/` mechanically replays them — one position at a time, entering on the candle *after* a Confirmed Buy signal (never the signal's own candle, which isn't fully known until it closes), exiting on Take Profit, Sell/Trend Reversal, or a 24h max-holding cap, and always reporting a buy & hold benchmark alongside the result so no number stands alone.

Run against ~1 year of real hourly Bitcoin data (Sept 2025 – Sept 2026, 8,585 rows, fetched from CoinCap and pushed through the actual dbt models in `OCI_GOLD.fct_crypto_intraday_prices`):

```
trades opened/closed:  122 / 122 (0 abandoned)
win rate:              45.9% (56/122)
mean return/trade:     -0.37% (net of 10 bps/side fees)
compounded return:     -38.61% (all trades chained)
t-stat:                -1.876 (not significant at naive 95%)

buy & hold over same period: -29.93%
```

**Honest reading:** over this specific period, the strategy loses money and does *worse* than simply holding Bitcoin — both are negative because it was a down year for BTC, but the rules add trading costs and whipsaw losses on top of the drawdown rather than avoiding it. The t-stat isn't statistically significant either way, and with a single asset over one year the 122 trades aren't independent draws (a multi-week trend clusters correlated wins or losses), so this isn't a rigorous significance test — it's a first honest check, and it fails to show an edge. Run it yourself: `python -m backtest --data <features.csv> --report`.

### 🇪🇸 Backtest: ¿La estrategia es realmente rentable?

Las reglas del DAX de arriba son umbrales fijos de indicadores técnicos, no un modelo ajustado a estos datos, así que se pueden probar directamente contra el histórico real sin necesitar una separación train/test. `backtest/` reproduce las reglas mecánicamente — una posición a la vez, entrando en la vela *siguiente* a una señal de Confirmed Buy (nunca en la vela de la propia señal, que no se conoce del todo hasta que cierra), saliendo por Take Profit, Sell/Trend Reversal, o un tope de 24h de holding máximo, y siempre reportando un benchmark de buy & hold junto al resultado para que ningún número quede solo.

Ejecutado contra ~1 año de datos horarios reales de Bitcoin (sept. 2025 – sept. 2026, 8,585 filas, obtenidas de CoinCap y procesadas por los modelos reales de dbt en `OCI_GOLD.fct_crypto_intraday_prices`):

```
operaciones abiertas/cerradas: 122 / 122 (0 abandonadas)
win rate:                      45.9% (56/122)
retorno medio/operación:       -0.37% (neto de comisiones de 10 bps por lado)
retorno compuesto:             -38.61% (todas las operaciones encadenadas)
t-stat:                        -1.876 (no significativo al 95% ingenuo)

buy & hold en el mismo período: -29.93%
```

**Lectura honesta:** en este período específico, la estrategia pierde dinero y lo hace *peor* que simplemente mantener Bitcoin — ambos son negativos porque fue un año bajista para BTC, pero las reglas suman costos de transacción y pérdidas por whipsaw encima de la caída en vez de evitarla. El t-stat tampoco es estadísticamente significativo, y con un solo activo durante un año las 122 operaciones no son extracciones independientes (una tendencia de varias semanas agrupa ganancias o pérdidas correlacionadas), así que esto no es una prueba de significancia rigurosa — es una primera verificación honesta, y no logra mostrar una ventaja. Corrélo usted mismo: `python -m backtest --data <features.csv> --report`.

