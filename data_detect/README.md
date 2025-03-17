# Data Feature Analysis Task

## Project Objective
Analyze the distribution of critical features in datasets, evaluate data integrity and coverage.

## Specific Tasks

### 1. Extract Critical Feature Names
- Read the "critical_data.csv" file in the root directory
- Filter rows where column D is marked as "Critical"
- Get names from column A as critical_features
- Output a list of all critical_features

### 2. Get Dataset Feature Information
- Scan all .tsv files in the /data directory
- Extract all column names from each tsv file
- Classify column names by file name
- Generate nodes_feature mapping table

### 3. Feature Coverage Analysis
- Check if all critical_features appear in nodes_features
  - Mark as 'y' if exists, and note which file it appears in
  - Mark as 'n' if not exists
- Calculate the data coverage of each feature in the original tsv files
  - Calculate the proportion of non-null values
  - Generate coverage report
  - Mark whether it is a critical_feature

## Expected Output
1. critical_feature list
2. nodes_feature mapping table
3. Feature existence comparison results (y/n)
4. Feature coverage statistical report

## Notes
- Need to handle file reading exceptions
- Consider data types when calculating coverage
- Output results in a clear and readable format

## Usage Instructions

### Environment Setup
```bash
# Activate virtual environment
source ~/gen3_env/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Run Script
```bash
python data_feature_analysis.py
```
```bash
python data_feature_analysis.py --threshold 0.3
```


### Output Files
After the script runs, the following files will be generated in the current directory:
1. `critical_features.csv` - Critical feature list
2. `nodes_features.csv` - Node feature mapping table
3. `feature_existence.csv` - Feature existence comparison results
4. `feature_coverage.csv` - Feature coverage statistical report