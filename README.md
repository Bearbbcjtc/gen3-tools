# Gen3-tools

A collection of utilities and tools for working with Gen3 data commons, data analysis, and node management.

## Project Overview

This repository contains several tools designed to work with Gen3 data commons:

1. **gen3utils**: Utilities for Gen3 commons management, including validation tools, ETL mapping, and portal configuration.
2. **data_detect**: Tools for analyzing data feature distribution, integrity, and coverage.
3. **replace-nodes**: Scripts for managing nodes in Gen3 commons.

## Components

### gen3utils

Utils for Gen3 commons management. Gen3utils includes a CLI and is intended to be used as an external facing tool for validating configuration, generating release notes, posting deployment changes, logging parser information, and more.

#### Features:
- Manifest.json validation
- ETL mapping validation
- Portal configuration validation
- Deployment change tracking
- Log parsing for CTDS log pipeline

#### Installation:
```bash
pip install gen3utils
```

### data_detect

Tools for analyzing the distribution of critical features in datasets, evaluating data integrity and coverage.

#### Features:
- Extract critical feature names from data files
- Scan and analyze dataset feature information
- Perform feature coverage analysis
- Generate comprehensive reports on data quality

#### Usage:
```bash
# Activate virtual environment
source ~/gen3_env/bin/activate

# Install dependencies
pip install -r data_detect/requirements.txt

# Run the analysis script
python data_detect/data_feature_analysis.py

# Run with custom threshold
python data_detect/data_feature_analysis.py --threshold 0.3
```

#### Output Files:
After running the script, the following files will be generated:
1. `critical_features.csv` - Critical feature list
2. `nodes_features.csv` - Node feature mapping table
3. `feature_existence.csv` - Feature existence comparison results
4. `feature_coverage.csv` - Feature coverage statistical report
5. `missing_critical_features.csv` - List of missing critical features
6. `critical_features_coverage.csv` - Coverage report for critical features

### replace-nodes

A utility for managing nodes in Gen3 data commons.

#### Features:
- Delete nodes from specified programs and projects
- Support for different Gen3 commons environments

#### Usage:
```bash
# Activate virtual environment
source ~/gen3_env/bin/activate

# Install dependencies
pip install gen3

# Run the script
python replace-nodes/replace-nodes.py
```

## Environment Setup

Most tools in this repository require a Gen3 environment:

```bash
# Create and activate a virtual environment
python -m venv ~/gen3_env
source ~/gen3_env/bin/activate

# Install common dependencies
pip install gen3
```

## Authentication

For tools that interact with Gen3 commons, authentication is required:

1. Create a credentials file in JSON format (see examples in replace-nodes directory)
2. Specify the path to the credentials file when running scripts

## Contributing

Please follow the established code style and include appropriate tests for new features.

## License

This project is licensed under the terms included in the LICENSE file. 