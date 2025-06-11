# DigitalTwin - Data Space

**DigitalTwin Data Sapce** is a Python package for creating, managing, and querying data spaces, with a focus on modular
data
pipelines and digital twin applications. It provides a flexible framework to define, schedule, and run data collectors,
harvesters, and handlers, supporting complex data workflows and dependencies.

---

## Features

- **Modular Components:** Define custom Collectors, Harvesters, and Handlers for your data workflows.
- **Configuration-Driven:** Easily configure your data pipeline using TOML files.
- **Dependency Management:** Automatically resolves and schedules component dependencies.
- **CLI Interface:** Run, schedule, and manage your data pipeline from the command line.
- **Extensible:** Add new data sources, processing steps, or outputs by implementing new components.

---

## Installation

```bash
pip install digitaltwin_dataspace
```

Or, for development:

```bash
git clone https://github.com/GaspardMerten/digitaltwin_dataspace.git
cd digitaltwin_dataspace
pip install -e .
```

**Dependencies:**

- Python 3.8+
- requests
- SQLAlchemy
- azure-storage-blob
- schedule
- dotenv

(See `pyproject.toml` for the full list.)

---

## Usage

### Command Line Interface

The main entry point is the `dt-dataspace` CLI:

```bash
dt-dataspace --config-folder path/to/config [options]
```

**Key options:**

- `--config-folder`: Path to the configuration folder (default: `config`)
- `--init-dependencies`: Run all harvesters in dependency order
- `--handlers`: List of handler names to run
- `--collectors`: List of collector names to run
- `--harvesters`: List of harvester names to run
- `--now`: Run harvesters or collectors once and exit
- `--port`: Port for the handlers server (default: 8888)
- `--host`: Host for the handlers server (default: localhost)
- `--allowed-hosts`: Allowed hosts for the handlers server
- `--log-level`: Set logging level (`DEBUG`, `INFO`, etc.)
- `--parquetize`: List of harvester names to run for parquet output

---

## Project Structure

```
digitaltwin_dataspace/
│
├── components/
│   ├── collector.py   # Base Collector class
│   ├── handler.py     # Base Handler class
│   └── harvester.py   # Base Harvester class
│
├── configuration/
│   ├── load.py        # Loads and parses component configuration
│   └── model.py       # Configuration data models
│
├── data/
│   ├── sync_db.py     # Database sync logic
│   ├── retrieve.py    # Data retrieval utilities
│   └── ...            # Other data management modules
│
├── cli.py             # Command-line interface
└── ...
```

---

## Components

- **Collector:**  
  Gathers data from external sources. Implement the `Collector` abstract class and its `run()` method.

- **Harvester:**  
  Processes or transforms collected data. Implement the `Harvester` abstract class and its `run()` method.

- **Handler:**  
  Serves or exposes processed data, e.g., via an API. Implement the `Handler` abstract class and its `run()` method.

You can add your own components by subclassing these base classes and registering them in your configuration.

---

## Configuration

Configuration is done via TOML files in your config folder (default: `config/`).  
Each file can define multiple collectors, harvesters, and handlers, specifying:

- `DATA_TYPE`, `DATA_FORMAT`
- `PATH` (Python import path to your component)
- `SCHEDULE` (optional, for scheduling)
- `SOURCE`, `DEPENDENCIES` (for workflow chaining)
- Other custom parameters

See `digitaltwin_dataspace/configuration/load.py` for all supported options.

---

## Author

Gaspard Merten  
[gaspard@norse.be](mailto:gaspard@norse.be)

---

## License

Attribution-NonCommercial-ShareAlike 4.0 International

---

## Links

- [Homepage](https://github.com/GaspardMerten/digitaltwin_dataspace)
- [Bug Tracker](https://github.com/GaspardMerten/digitaltwin_dataspace/issues)

--- 