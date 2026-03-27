# 📊 Quantitative Market Intelligence & Signals Dashboard
**End-to-End Analytics Portfolio: From API to Insights**

---

## 🇪🇸 Resumen del Proyecto (Spanish)
Este proyecto es una solución integral de inteligencia de mercado que automatiza la extracción, transformación y visualización de datos de criptomonedas en tiempo real. Implementa un pipeline robusto de datos (ELT) para generar señales de trading basadas en indicadores estadísticos (Z-Score) y técnicos (RSI).

### 🚀 Stack Tecnológico
* **Extracción:** Python & CoinCap API.
* **Orquestación:** Apache Airflow.
* **Transformación:** dbt (Data Build Tool).
* **Data Warehouse:** Oracle Autonomous Data Warehouse (ADW).
* **Visualización:** Power BI (Advanced DAX).

### 💡 Lógica de Negocio (Señales)
El dashboard identifica automáticamente:
* **Confirmación de Compra:** Cruce de Media Móvil + RSI bajo.
* **Toma de Ganancia:** Picos extremos de Z-Score (estadística) y RSI sobrecomprado.
* **Cálculo de ROI:** Seguimiento automático de rentabilidad entre señales de entrada y salida.

---

## 🇺🇸 Project Overview (English)
This project is an end-to-end market intelligence solution that automates the extraction, transformation, and visualization of real-time cryptocurrency data. It implements a robust ELT pipeline to generate trading signals based on statistical (Z-Score) and technical (RSI) indicators.

### 🚀 Technical Stack
* **Extraction:** Python & CoinCap API.
* **Orchestration:** Apache Airflow.
* **Transformation:** dbt (Data Build Tool).
* **Data Warehouse:** Oracle Autonomous Data Warehouse (ADW).
* **Visualization:** Power BI (Advanced DAX).

### 💡 Business Logic (Signals)
The dashboard automatically identifies:
* **Confirmed Buy:** Moving Average crossover + Low RSI.
* **Take Profit:** Extreme Z-Score peaks (statistical floor/ceiling) and Overbought RSI.
* **ROI Calculation:** Automatic tracking of profitability between entry and exit timestamps.

---

## 🛠️ Architecture & Core DAX / Arquitectura y DAX
*Example of the advanced logic implemented to identify market states:*

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

// 3. Evaluate real-time conditions
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