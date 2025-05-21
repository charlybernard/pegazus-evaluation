# pegazus-evaluation

This repository contains code and experiments for evaluating **PeGazUs** (PErpetual GAZeteer of approach-address UtteranceS), a method for reconstructing the evolution of geographical entities (e.g., addresses) from heterogeneous and fragmentary data.

## 📁 Repository Structure

```
pegazus-evaluation/
├── data/                  # Input data used for evaluation (e.g., GeoJSON, CSV)
├── code/                  # Python scripts to run the evaluations
├── configs/               # Config files
├── create_graphs/         # Jupyter notebooks for to create graphs
├── LICENCE.md             # License of the projects
└── README.md              # Project description and instructions
```

## 🚀 Getting Started

Ensure to build a `db_config.ini` file located in `configs` folder to connect a Postgres database for the first evaluation.
```
[postgresql]
host = localhost
port = 5432
database = my_database
user = my_user
password = my_secure_password
```

### Running the Evaluation

...

## 📊 Evaluation Methodology

The evaluation is based on reconstructing **lifelines** of `housenumber` entities using multiple time-stamped data sources such as:
- Cadastre
- Vasserot
- Jacoubet
- Atlas Municipal
- Base Adresse Nationale (BAN)
- OpenStreetMap (OSM)

The lifeline of a `housenumber` is a chain of attestations, ordered temporally, e.g.,

```
vasserot → ban → osm
```

Synthetic data is also injected to test robustness against **fragmentary data** (e.g., partial events or states that don’t contradict the known evolution).

We compare:
- Ground truth vs. graph results
- Presence and ordering of versions
- IoU (Intersection over Union) metric over matched `housenumber` evolutions

## 👤 Author

[Charly Bernard](https://github.com/charlybernard)
