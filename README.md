# iFood-like Medallion Pipeline (Databricks Serverless + Unity Catalog + Power BI)

![Medallion Architecture](diagrams/medallion_architecture.png)

Pipeline **end-to-end de Engenharia de Dados** simulando um cenário de pedidos em “quase tempo real” (estilo iFood), utilizando **Databricks Serverless**, **Unity Catalog** e **Arquitetura Medallion (Bronze / Silver / Gold)**, com camada final **BI-ready para Power BI**.

Este projeto foi construído com foco em **padrões reais de produção**, incluindo ingestão incremental, tolerância a dados inválidos, deduplicação determinística, MERGE incremental e modelagem analítica.

---

## 🎯 Objetivos do Projeto

- Simular ingestão de eventos OLTP em fluxo contínuo (pedidos e mudanças de status)
- Implementar arquitetura **Medallion** de forma prática
- Trabalhar com **restrições reais** do Databricks Serverless e Unity Catalog
- Entregar dados **prontos para consumo analítico (Power BI)**
- Demonstrar competências de **Engenharia de Dados**, não apenas análise

---

## 🏗️ Arquitetura (Medallion)

**Landing (CSV batches em Volume)**  
→ **Bronze** – eventos limpos e resilientes (append)  
→ **Silver Events Dedup** – eventos deduplicados por `event_id`  
→ **Silver Orders State** – estado atual do pedido (SCD Type 1)  
→ **Gold** – modelo analítico otimizado para BI

Principais características:
- Streaming com **AvailableNow** (compatível com Serverless)
- Armazenamento em **Unity Catalog Volumes** (sem DBFS root público)
- Uso de `_metadata.file_path` no lugar de `input_file_name`

---

## 🧰 Stack Tecnológica

- **Databricks Serverless**
- **Apache Spark (Structured Streaming)**
- **Delta Lake**
- **Unity Catalog**
- **Power BI**
- **Python / SQL**

---

## 📂 Estrutura do Repositório
ifood-like-medallion-databricks/
│
├── notebooks/
│ ├── 01_landing/
│ │ └── 01_generator_landing.py
│ │
│ ├── 02_bronze/
│ │ └── 02_bronze_stream.py
│ │
│ ├── 03_silver/
│ │ ├── 03a_silver_events_dedup_incremental.py
│ │ └── 03b_silver_orders_state_merge.py
│ │
│ └── 04_gold/
│ └── 04_gold_powerbi_ready.py
│
├── diagrams/
│ └── medallion_architecture.png
│
├── powerbi/
│ └── ifood_dashboard.pbix (opcional)
│
├── README.md
├── data_dictionary.md
└── LICENSE
