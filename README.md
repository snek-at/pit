<p align="center">
  <a href="https://snek.at/" target="_blank" rel="noopener noreferrer">
    <img src="https://user-images.githubusercontent.com/26285351/208744045-1ee54b94-5ab6-41dc-a70e-b604fba24e56.gif" alt="SNEK Logo" height="150">
  </a>
</p>

<h3 align="center">Snek Toolbox</h3>

<p align="center">
  This is a collection of handy scripts used in <a href="https://github.com/snek-at/functions" target="blank">snek-at/functions</a> provided by snek-at.
</p>

# Snek Pit

This application allows users to create and manage parquet files using DuckDB and Pandas. The application has the following functionality:

- `dump` command: Takes json data and saves it to a parquet file
- `retrieve` command: Loads a parquet file and returns the data as a json object
- `search` command: Search the specific column and value in the parquet file and returns the data as a json object

### Requirements

- Python 3
- DuckDB
- Pandas

### Installation

```bash
pip install duckdb pandas
```

### Usage

```bash
python script.py [command] [path/to/file.parquet] [data.json] [column_name1=value1] [column_name2=value2]
```

### Example

```bash
# Loads a parquet file and returns the data as a json object
python script.py retrieve path/to/file.parquet

# Saves json data to a parquet file
python script.py dump path/to/file.parquet data.json

# Search the specific column and value in the parquet file and returns the data as a json object
python script.py search path/to/file.parquet column_name1=value1 column_name2=value2
```

SPDX-License-Identifier: (EUPL-1.2)
Copyright © 2019-2022 snek.at
