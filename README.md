# Stock-Price-Predictor
# 📈 Stock Price Predictor Dashboard

![Docker](https://img.shields.io/badge/Docker-%E2%9C%93-blue)
![Cassandra](https://img.shields.io/badge/Cassandra-%E2%9C%93-success)
![Kafka](https://img.shields.io/badge/Kafka-%E2%9C%93-orange)
![Streamlit](https://img.shields.io/badge/Streamlit-%E2%9C%93-green)

A real-time stock price prediction system using Kafka, Cassandra, Spark, and Streamlit — all containerized with Docker.

---

## 🚀 Project Progress

We started this project from scratch with an idea — to stream stock data and build a smart, real-time prediction engine.

**Project Status: 60% Complete ✅**

### 📊 Progress Overview

```mermaid
gantt
    title Project Progress
    dateFormat  YYYY-MM-DD
    section Infrastructure
    Docker Setup             :done,    a1, 2025-08-01, 1d
    Kafka Setup              :done,    a2, 2025-08-01, 1d
    Cassandra Setup          :done,    a3, 2025-08-01, 1d

    section Dashboard
    Streamlit UI             :active,  b1, 2025-08-02, 3d

    section Processing
    Spark Batch Jobs         :         c1, 2025-08-10, 3d
    ML Predictions (LSTM)    :         c2, 2025-08-13, 3d
