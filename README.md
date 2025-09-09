<!-- START OF README INTRO -->

# 🚀 SQL Data Warehouse Project  

## 📖 Project Overview  
This project demonstrates the end-to-end design and implementation of a **modern SQL-based data warehouse** using the Medallion Architecture:  

- **🏗️ Data Architecture:** Bronze, Silver, and Gold layers for structured pipelines.  
- **⚙️ ETL Pipelines:** Extract, transform, and load data from source systems into the warehouse.  
- **📊 Data Modeling:** Fact and dimension tables optimized for analytical queries.  
- **📈 Analytics & Reporting:** SQL-driven dashboards and reports that provide actionable insights.  
---

## 🛠️ Tools & Resources  
- **Datasets:** Provided as CSV files (ERP + CRM).  
- **SQL Server Express:** Lightweight server to host database.  
- **SSMS (SQL Server Management Studio):** GUI for database management.  
- **GitHub Repository:** Version control and collaboration.  
- **DrawIO:** For architecture, flows, and schema diagrams.  
- **Notion Template:** Step-by-step project guide with all phases and tasks.  

---

## 📌 Project Requirements  

### **Data Warehouse (Data Engineering)**  
**Objective:** Build a modern warehouse to consolidate sales data and enable analytical reporting.  

**Key Specs:**  
- Import data from **ERP & CRM CSV sources**.  
- Cleanse and resolve data quality issues.  
- Integrate sources into a single analytical model.  
- Focus on **latest datasets only** (no historization).  
- Provide **clear documentation** for both business and technical teams.  

### **BI & Analytics (Data Analysis)**  
**Objective:** Deliver SQL-based analytics for:  
- Customer behavior 🧑‍🤝‍🧑  
- Product performance 📦  
- Sales trends 💹  

These insights empower stakeholders with data-driven decisions.  


---

## 📂 Repository Structure  
```plaintext
data-warehouse-project/
├── datasets/              # Raw ERP & CRM datasets
├── docs/                  # Documentation & diagrams
├── scripts/               # SQL ETL & transformation scripts
├── tests/                 # Test cases & validation files
├── README.md              # Project overview

