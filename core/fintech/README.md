<a name="readme-top"></a>

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about">About</a>
    </li>
    <li>
      <a href="#structure">Structure</a>
      <ul>
        <li><a href="#dags">Dags</a></li>
        <li><a href="#dbt">DBT</a></li>
        <li><a href="#custom-modules">Custom Modules</a></li>
        <li><a href="#configs">Configs</a></li>
      </ul>
    </li>
  </ol>
</details>



<!-- ABOUT -->
## About

Analytics team repository for:
- storage of data processing and analysis functions in a single place,
- setup of tasks in airflow,
- building data models in dbt.

Links (NDA):
- Analytics Layer
- Analytics DWH

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- STRUCTURE -->
## Structure

Below is the structure of files and folders in the repository

### DAGS

Each DAG included:

**Description**
You can open any DAG to be able to see this or just hover on it

**Tags**
At least 4 tags for each DAG if applicable:

- *Source*: the name of the source of data that are used in Data Pipeline (for example: ch_prod2)
- *Mode*: method of updating data form the source. Accepted values: 
  - rewrite (rewrite data each time), 
  - upsert (add new data and update old records),
  - insert (add new data).
- *Action*: type of action with data. Accepted values: 
  - load (load data from the source to DWH), 
  - transform (transform raw data into data marts),
  - update (update data in the source), 
  - transfer (transfer data from one source to second).
- *Technology*: what type of technology/language is used in the DAG. 
  - sql (if we use sql queries in the DAG),
  - api (get or push data via API).

**Naming**:
There is template for each DAG’s name:

[data layout]\_[source]\_[source’s entity]\_dag_[dag’s number]

_example_: raw_data_ch_prod2_arb_ops_dag_1

**DAG's structure**

- alerts
  - monitoring _(for monitoring notifications)_
  - reports _(for sending reports)_
- EL_data _(Extract and Load data)_
  - internal _(Extract and Load data from internal sources)_
  - external _(Extract and Load data from external sources)_
- T_data _(Transform data into marts)_

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### DBT

Used as a tool for Transform data to data mart and tests.
Each group of models has own yaml config with all documentation.

Models:
- staging _(for pre-defined data from the sources)_
- marts_roxana _(for joined and aggregation Roxana data)_
- marts_cornelia _(for joined and aggregation Cornelia data)_
- marts_financial _(for joined and aggregation Financial data)_

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Custom Modules

Python modules with functions that can be used in our proccesses

Modules:
- etherscan _(functions to extract data through the etherscan api)_
- internal_clickhouse _(functions to work with internal clickhouses)_
- internal_data_processing _(common functions our infr and proccesses)_
- internal_slack _(functions to work with internal slack)_

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Configs

Folder with our templates and configs for services

- dags_template.py _(template for new DAGs)_
- extract_clickhouse.yml _(config with tables that should be injected from our clickhouses)_
- dbt_model_configs.yml _(config with template for dbt modeling configuration)_

<p align="right">(<a href="#readme-top">back to top</a>)</p>
