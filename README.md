# *ETL Midterm – Snit_123*

## *1. Project Overview*  
*This repository delivers a structured ETL pipeline that:*  
*- **Extracts** raw and incremental order data*  
*- **Transforms** the data via cleaning, enrichment, and type standardization*  
*- **Loads** the processed data into efficient Parquet storage*  

*The goal is to demonstrate a reproducible, analytical data flow.*

---

## *2. ETL Phases*

### *Extract*  
*- Notebook: `etl_extract.ipynb`*  
*- Loads `raw_data.csv` and `incremental_data.csv`*  
*- Uses `.head()` and `.info()` to inspect data and document observations*  
*- Saves raw copies in the `data/` folder*

### *Transform*  
*- Notebook: `etl_transform.ipynb`*  
*- Performs ≥4 transformations:*  
  *- Removes duplicates*  
  *- Handles missing values (context-aware fills)*  
  *- Converts data types (`order_date`, `order_id`, `product`, `region`)*  
  *- Adds `total_price` enrichment and revenue categorization*  
*- Includes before/after snapshots and explanations*

### *Load*  
*- Notebook: `etl_load.ipynb`*  
*- Reads `transformed_full.csv` and `transformed_incremental.csv`*  
*- Saves cleaned data as Snappy-compressed Parquet files (`loaded/`)*  
*- Verifies outputs using `pd.read_parquet().head()`*

### *Visualization*

*Notebook: `Visualization.ipynb`*  
*Purpose: Visualize total revenue trends for both full and incremental datasets.*  

**Key Chart:**  
`monthly_revenue = transformed_full.groupby(['month', 'product'])['total_price'].sum().unstack(fill_value=0)`  
*This creates a month-by-product revenue matrix using `groupby()` + `unstack()`, ideal for bar chart plotting — a best practice for reshaping data in pandas.* 
![alt text](image.png)
![alt text](image-1.png)

*The notebook uses matplotlib to produce grouped bar charts that compare monthly revenue across products, providing clear insights into trends over time.* 


## *3. Folder Structure*
```plaintext
ETL_Midterm_Snit_552/
├── data/
│   ├── raw_data.csv
│   └── incremental_data.csv
├── transformed/
│   ├── transformed_full.csv
│   └── transformed_incremental.csv
├── loaded/
│   ├── full_data.parquet
│   └── incremental_data.parquet
├── etl_extract.ipynb
├── etl_transform.ipynb
├── etl_load.ipynb
├── Visualization.ipynb
├── README.md                               # Project documentation
└── .gitignore                              # Git ignore file

```
## *4. Tools Used*  
*- Python & pandas*  
*- Jupyter Notebooks*  
*- Parquet (Snappy compression)*  

## *5. How to Reproduce*  
```bash
git clone https://github.com/yourusername/DSA2040A_ETL_Midterm_Snit_123.git
cd ETL_Midterm_Snit_123
pip install pandas pyarrow matplotlib
# Run: etl_extract → etl_transform → etl_load → Visualization

```
 ## *Outputs*

- *`transformed/`*  
  - *`transformed_full.csv`: full cleaned dataset (91 rows × 10 columns)*  
  - *`transformed_incremental.csv`: incremental updates (9 rows × 10 columns)*  
- *`loaded/`*  
  - *`full_data.parquet`: Snappy‑compressed Parquet file for full dataset*  
  - *`incremental_data.parquet`: Snappy‑compressed Parquet file for incremental dataset*  
- *`Visualization.ipynb`: includes charts such as:*  
  • *Monthly revenue by product*  



