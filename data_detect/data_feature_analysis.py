#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Data Feature Analysis Tool

This script is used to analyze the distribution of critical features in datasets,
evaluate data integrity and coverage.
"""

import os
import pandas as pd
import numpy as np
import csv
import glob
import argparse
from typing import List, Dict, Tuple, Set, Any
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataFeatureAnalyzer:
    """Data Feature Analyzer Class"""
    
    def __init__(self, critical_data_path: str, data_dir: str, coverage_threshold: float = 0.8):
        """
        Initialize the data feature analyzer
        
        Parameters:
            critical_data_path: Path to the critical_data_v2.csv file
            data_dir: Path to the directory containing tsv files
            coverage_threshold: Threshold for considering a feature's coverage as sufficient (default: 0.8)
        """
        self.critical_data_path = critical_data_path
        self.data_dir = data_dir
        self.coverage_threshold = coverage_threshold
        self.critical_features: List[str] = []  # 原始关键特征
        self.mapped_critical_features: List[str] = []  # 映射后的关键特征
        self.feature_mapping: Dict[str, str] = {}  # 原始特征到映射特征的映射关系
        self.nodes_features: Dict[str, List[str]] = {}
        self.feature_existence: Dict[str, Dict[str, Any]] = {}
        self.coverage_report: Dict[str, Dict[str, Any]] = {}
        # 新增：存储缺失的关键特征
        self.missing_critical_features: List[str] = []
        # 新增：存储存在的关键特征的覆盖率
        self.critical_features_coverage: Dict[str, Dict[str, Any]] = {}
        # 新增：存储存在的关键特征
        self.existing_critical_features: List[str] = []
    
    def extract_critical_features(self) -> Tuple[List[str], List[str], Dict[str, str]]:
        """
        Extract critical feature names from the critical_data_v2.csv file
        
        Returns:
            Tuple containing:
            - List of original critical feature names
            - List of mapped critical feature names
            - Dictionary mapping original features to mapped features
        """
        try:
            logger.info(f"Extracting critical features from {self.critical_data_path}...")
            df = pd.read_csv(self.critical_data_path, low_memory=False)
            
            # Filter rows where column 4 (index 3) is marked as "Critical"
            critical_rows = df[df.iloc[:, 3] == "Critical"]
            
            # Get names from column 1 (index 0) for the corresponding rows
            self.critical_features = critical_rows.iloc[:, 0].tolist()
            
            # Get mapped feature names from 'property' column (index 10)
            property_values = critical_rows.iloc[:, 10].tolist()
            
            # Create mapping dictionary
            self.feature_mapping = {}
            for i, feature in enumerate(self.critical_features):
                if isinstance(feature, str) and feature.strip():
                    property_value = property_values[i]
                    if isinstance(property_value, str) and property_value.strip():
                        self.feature_mapping[feature] = property_value
                    else:
                        # If no mapping, use original feature name
                        self.feature_mapping[feature] = feature
            
            # Get unique mapped feature names
            self.mapped_critical_features = list(set(self.feature_mapping.values()))
            
            # Remove possible empty values
            self.critical_features = [f for f in self.critical_features if isinstance(f, str) and f.strip()]
            self.mapped_critical_features = [f for f in self.mapped_critical_features if isinstance(f, str) and f.strip()]
            
            logger.info(f"Successfully extracted {len(self.critical_features)} original critical features")
            logger.info(f"Successfully mapped to {len(self.mapped_critical_features)} unique critical features")
            
            return self.critical_features, self.mapped_critical_features, self.feature_mapping
            
        except Exception as e:
            logger.error(f"Error extracting critical features: {str(e)}")
            return [], [], {}
    
    def get_nodes_features(self) -> Dict[str, List[str]]:
        """
        Extract feature information from tsv files in the data directory
        
        Returns:
            Feature mapping table classified by file name
        """
        try:
            logger.info(f"Scanning tsv files in {self.data_dir} directory...")
            tsv_files = glob.glob(os.path.join(self.data_dir, "*.tsv"))
            
            for tsv_file in tsv_files:
                file_name = os.path.basename(tsv_file)
                logger.info(f"Processing file: {file_name}")
                
                try:
                    # Read the first line of the TSV file to get column names
                    with open(tsv_file, 'r', encoding='utf-8') as f:
                        header = f.readline().strip().split('\t')
                    
                    # Store column names
                    self.nodes_features[file_name] = header
                    
                except Exception as e:
                    logger.error(f"Error processing file {file_name}: {str(e)}")
            
            logger.info(f"Successfully processed {len(self.nodes_features)} tsv files")
            return self.nodes_features
            
        except Exception as e:
            logger.error(f"Error getting node features: {str(e)}")
            return {}
    
    def analyze_feature_existence(self) -> Dict[str, Dict[str, Any]]:
        """
        Analyze whether mapped critical features exist in node features
        
        Returns:
            Feature existence analysis results
        """
        try:
            logger.info("Analyzing feature existence...")
            
            # Ensure critical features and node features have been extracted
            if not self.mapped_critical_features:
                self.extract_critical_features()
            
            if not self.nodes_features:
                self.get_nodes_features()
            
            # 重置缺失的关键特征列表和存在的关键特征列表
            self.missing_critical_features = []
            self.existing_critical_features = []
            
            # Analyze whether each mapped critical feature exists in node features
            for feature in self.mapped_critical_features:
                self.feature_existence[feature] = {
                    "exists": "n",
                    "files": []
                }
                
                for file_name, features in self.nodes_features.items():
                    if feature in features:
                        self.feature_existence[feature]["exists"] = "y"
                        self.feature_existence[feature]["files"].append(file_name)
                
                # 如果特征不存在于任何文件中，添加到缺失列表
                if self.feature_existence[feature]["exists"] == "n":
                    self.missing_critical_features.append(feature)
                else:
                    self.existing_critical_features.append(feature)
            
            logger.info(f"Feature existence analysis completed. Found {len(self.missing_critical_features)} missing critical features")
            return self.feature_existence
            
        except Exception as e:
            logger.error(f"Error analyzing feature existence: {str(e)}")
            return {}
    
    def calculate_coverage(self) -> Dict[str, Dict[str, Any]]:
        """
        Calculate the data coverage of each feature in the original tsv files
        
        Returns:
            Feature coverage statistical report
        """
        try:
            logger.info("Calculating feature coverage...")
            
            # Ensure critical features and node features have been extracted
            if not self.mapped_critical_features:
                self.extract_critical_features()
            
            if not self.nodes_features:
                self.get_nodes_features()
            
            # 重置关键特征覆盖率字典
            self.critical_features_coverage = {}
            
            # Calculate coverage for each feature in each file
            for file_name, features in self.nodes_features.items():
                file_path = os.path.join(self.data_dir, file_name)
                
                try:
                    # Read TSV file
                    df = pd.read_csv(file_path, sep='\t', low_memory=False)
                    
                    # Calculate the proportion of non-null values for each column
                    for feature in features:
                        if feature not in self.coverage_report:
                            self.coverage_report[feature] = {}
                        
                        if feature in df.columns:
                            non_null_count = df[feature].count()
                            total_count = len(df)
                            coverage = non_null_count / total_count if total_count > 0 else 0
                            
                            self.coverage_report[feature][file_name] = {
                                "coverage": coverage,
                                "is_critical": feature in self.mapped_critical_features
                            }
                            
                            # 如果是关键特征，添加到关键特征覆盖率字典
                            if feature in self.mapped_critical_features:
                                if feature not in self.critical_features_coverage:
                                    self.critical_features_coverage[feature] = {}
                                
                                self.critical_features_coverage[feature][file_name] = {
                                    "coverage": coverage,
                                    "non_null_count": non_null_count,
                                    "total_count": total_count
                                }
                
                except Exception as e:
                    logger.error(f"Error calculating coverage for file {file_name}: {str(e)}")
            
            logger.info("Feature coverage calculation completed")
            return self.coverage_report
            
        except Exception as e:
            logger.error(f"Error calculating coverage: {str(e)}")
            return {}
    
    def generate_reports(self, output_dir: str = ".") -> None:
        """
        Generate analysis reports
        
        Parameters:
            output_dir: Output directory path
        """
        try:
            logger.info("Generating analysis reports...")
            
            # Ensure all analyses have been completed
            if not self.mapped_critical_features:
                self.extract_critical_features()
            
            if not self.nodes_features:
                self.get_nodes_features()
            
            if not self.feature_existence:
                self.analyze_feature_existence()
            
            if not self.coverage_report:
                self.calculate_coverage()
            
            # Generate critical feature list report
            critical_features_path = os.path.join(output_dir, "critical_features.csv")
            with open(critical_features_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["Original Feature Name", "Mapped Feature Name"])
                for feature, mapped_feature in self.feature_mapping.items():
                    writer.writerow([feature, mapped_feature])
            
            # Generate node feature mapping table report
            nodes_features_path = os.path.join(output_dir, "nodes_features.csv")
            with open(nodes_features_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["File Name", "Feature Name"])
                for file_name, features in self.nodes_features.items():
                    for feature in features:
                        writer.writerow([file_name, feature])
            
            # Generate feature existence comparison result report
            existence_path = os.path.join(output_dir, "feature_existence.csv")
            with open(existence_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["Feature Name", "Exists", "Files"])
                for feature, info in self.feature_existence.items():
                    writer.writerow([feature, info["exists"], ", ".join(info["files"])])
            
            # Generate feature coverage statistical report
            coverage_path = os.path.join(output_dir, "feature_coverage.csv")
            with open(coverage_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["Feature Name", "File Name", "Coverage", "Is Critical Feature"])
                for feature, file_info in self.coverage_report.items():
                    for file_name, info in file_info.items():
                        writer.writerow([
                            feature,
                            file_name,
                            f"{info['coverage']:.2%}",
                            "Yes" if info["is_critical"] else "No"
                        ])
            
            # 新增：生成缺失的关键特征报告
            missing_critical_path = os.path.join(output_dir, "missing_critical_features.csv")
            with open(missing_critical_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["Missing Critical Feature"])
                for feature in self.missing_critical_features:
                    writer.writerow([feature])
            
            # 新增：生成存在的关键特征覆盖率报告
            critical_coverage_path = os.path.join(output_dir, "critical_features_coverage.csv")
            with open(critical_coverage_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["Critical Feature", "File Name", "Coverage", "Non-Null Count", "Total Count"])
                for feature, file_info in self.critical_features_coverage.items():
                    for file_name, info in file_info.items():
                        writer.writerow([
                            feature,
                            file_name,
                            f"{info['coverage']:.2%}",
                            info["non_null_count"],
                            info["total_count"]
                        ])
            
            logger.info(f"Analysis reports generated in {output_dir}")
            
        except Exception as e:
            logger.error(f"Error generating reports: {str(e)}")
    
    def print_summary(self) -> None:
        """
        Print a summary of the analysis results to the console
        """
        try:
            # Ensure all analyses have been completed
            if not self.mapped_critical_features:
                self.extract_critical_features()
            
            if not self.nodes_features:
                self.get_nodes_features()
            
            if not self.feature_existence:
                self.analyze_feature_existence()
            
            if not self.coverage_report:
                self.calculate_coverage()
            
            # Count features with coverage above threshold
            features_above_threshold = 0
            for feature, file_info in self.critical_features_coverage.items():
                # Calculate average coverage across all files for this feature
                total_coverage = 0
                file_count = 0
                for file_name, info in file_info.items():
                    total_coverage += info["coverage"]
                    file_count += 1
                
                avg_coverage = total_coverage / file_count if file_count > 0 else 0
                if avg_coverage >= self.coverage_threshold:
                    features_above_threshold += 1
            
            # Get all unique features from all files
            all_features = set()
            for file_name, features in self.nodes_features.items():
                all_features.update(features)
            
            # Print summary
            print("\n" + "="*80)
            print("DATA FEATURE ANALYSIS SUMMARY")
            print("="*80)
            print(f"1. Original critical features extracted: {len(self.critical_features)}")
            print(f"2. Unique mapped critical features: {len(self.mapped_critical_features)}")
            print(f"3. Total features in data files: {len(all_features)}")
            print(f"4. Critical features missing from data: {len(self.missing_critical_features)}")
            print(f"5. Critical features present in data: {len(self.existing_critical_features)}")
            print(f"6. Critical features with coverage >= {self.coverage_threshold:.2%}: {features_above_threshold}")
            print("="*80 + "\n")
            
        except Exception as e:
            logger.error(f"Error printing summary: {str(e)}")

# 如果直接运行此脚本
if __name__ == "__main__":
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="Data Feature Analysis Tool")
    parser.add_argument("--critical-data", default="critical_data_v2.csv", help="Path to the critical data CSV file")
    parser.add_argument("--data-dir", default="data", help="Path to the directory containing TSV files")
    parser.add_argument("--threshold", type=float, default=0.8, help="Coverage threshold (0.0-1.0)")
    parser.add_argument("--output-dir", default=".", help="Output directory for reports")
    
    args = parser.parse_args()
    
    try:
        # 创建分析器实例
        analyzer = DataFeatureAnalyzer(
            critical_data_path=args.critical_data,
            data_dir=args.data_dir,
            coverage_threshold=args.threshold
        )
        
        # 提取关键特征
        original_features, mapped_features, feature_mapping = analyzer.extract_critical_features()
        
        # 获取节点特征
        nodes_features = analyzer.get_nodes_features()
        
        # 分析特征存在性
        feature_existence = analyzer.analyze_feature_existence()
        
        # 计算覆盖率
        coverage_report = analyzer.calculate_coverage()
        
        # 生成报告
        analyzer.generate_reports(output_dir=args.output_dir)
        
        # 打印摘要
        analyzer.print_summary()
        
        logger.info("Analysis completed successfully")
        
    except Exception as e:
        logger.error(f"Error running analysis: {str(e)}") 