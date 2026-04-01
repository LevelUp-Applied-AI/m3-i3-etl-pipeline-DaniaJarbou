[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Nvxy3054)
# ETL Pipeline — Amman Digital Market

## Overview

<!-- What does this pipeline do? -->
The pipeline performs four main steps:
1. **Extract**: Pulls data from 4 tables (`customers`, `products`, `orders`, `order_items`).
2. **Transform**: Joins the tables, calculates revenue, and filters out cancelled or incorrect orders.
3. **Validate**: Checks the data quality (no nulls, unique IDs, positive revenue).
4. **Load**: Saves the final report as a CSV file and creates a fresh table in the database.


## Setup

**Create and Start the Container**: 
   I used Docker to run a PostgreSQL instance on port **5433**:
   ```bash
   docker run -d --name postgres-m3-int `
     -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres `
     -e POSTGRES_DB=amman_market `
     -p 5433:5432 -v pgdata_m3_int:/var/lib/postgresql/data `
     postgres:15-alpine
```
2. Load schema and data:
   Get-Content schema.sql | docker exec -i postgres-m3-int psql -U postgres -d amman_market
Get-Content seed_data.sql | docker exec -i postgres-m3-int psql -U postgres -d amman_market


3. Install dependencies: `pip install -r requirements.txt`

## How to Run

```bash
python etl_pipeline.py
```

## Output

The final report is in output/customer_analytics.csv. It shows each customer's total orders, total revenue, and average spending.

## Quality Checks

4 checks to ensure the data is 100% correct:

No Nulls: Every customer has a name.

 Positive Revenue: Revenue is always > 0.

 Unique IDs: No duplicate customer records.

 Valid Orders: Every customer has at least one successful order.


## License

This repository is provided for educational use only. See [LICENSE](LICENSE) for terms.

You may clone and modify this repository for personal learning and practice, and reference code you wrote here in your professional portfolio. Redistribution outside this course is not permitted.
