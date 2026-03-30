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