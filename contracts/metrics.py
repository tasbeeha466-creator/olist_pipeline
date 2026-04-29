import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
from datetime import datetime
from typing import Dict
import pandas as pd
from config.local_config import DATA_DIR, LOGS_DIR


class DataQualityReport:
    def init(self, table_name: str, df: pd.DataFrame):
        self.table_name = table_name
        self.df = df
        self.timestamp = datetime.now().isoformat()
        self.report = {}

    def compute(self) -> Dict:
        self.report = {
            "table_name": self.table_name,
            "timestamp": self.timestamp,
            "row_count": len(self.df),
            "column_count": len(self.df.columns),
            "completeness": self._completeness(),
            "duplicates": self._duplicates(),
            "numeric_stats": self._numeric_stats(),
            "overall_score": 0.0
        }
        self.report["overall_score"] = self._overall_score()
        return self.report

    def _completeness(self) -> Dict:
        result = {}
        for col in self.df.columns:
            null_count = int(self.df[col].isna().sum())
            total = len(self.df)
            result[col] = {
                "null_count": null_count,
                "null_pct": round(null_count / total * 100, 2),
                "completeness_pct": round((total - null_count) / total * 100, 2)
            }
        return result

    def _duplicates(self) -> Dict:
        total = len(self.df)
        full_dups = int(self.df.duplicated().sum())
        return {
            "full_duplicate_rows": full_dups,
            "full_duplicate_pct": round(full_dups / total * 100, 2),
            "unique_rows": total - full_dups
        }

    def _numeric_stats(self) -> Dict:
        result = {}
        numeric_cols = self.df.select_dtypes(include=["number"]).columns
        for col in numeric_cols:
            s = self.df[col].dropna()
            if len(s) == 0:
                continue
            result[col] = {
                "mean": round(float(s.mean()), 2),
                "median": round(float(s.median()), 2),
                "std": round(float(s.std()), 2),
                "min": round(float(s.min()), 2),
                "max": round(float(s.max()), 2),
                "negative_count": int((s < 0).sum()),
                "zero_count": int((s == 0).sum())
            }
        return result

    def _overall_score(self) -> float:
        completeness_scores = [
            v["completeness_pct"]
            for v in self.report["completeness"].values()
        ]
        avg_completeness = sum(completeness_scores) / len(completeness_scores) if completeness_scores else 0
        dup_penalty = self.report["duplicates"]["full_duplicate_pct"]
        score = avg_completeness - dup_penalty
        return round(max(0, min(100, score)), 2)

    def save(self):
        path = os.path.join(LOGS_DIR, f"quality_{self.table_name}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.report, f, indent=2, ensure_ascii=False)

    def print_summary(self):
        score = self.report.get("overall_score", 0)
        rows = self.report.get("row_count", 0)
        dups = self.report["duplicates"]["full_duplicate_rows"]
        worst_cols = sorted(
            self.report["completeness"].items(),
            key=lambda x: x[1]["completeness_pct"]
        )[:3]

        print(f"\n{'='*50}")
        print(f"Quality Report: {self.table_name}")
        print(f"{'='*50}")
        print(f"Overall Score:  {score}/100")
        print(f"Total Rows:     {rows:,}")
        print(f"Duplicates:     {dups:,}")
        print(f"Worst Columns (completeness):")
        for col, stats in worst_cols:
            print(f"  {col}: {stats['completeness_pct']}% complete")


def run_quality_checks() -> Dict[str, DataQualityReport]:
    tables = [
        "orders", "items", "customers", "payments",
        "reviews", "products", "sellers"
    ]
    reports = {}
    for table in tables:
        path = os.path.join(DATA_DIR, f"sample_{table}.csv")
        if not os.path.exists(path):
            continue
        df = pd.read_csv(path)
        report = DataQualityReport(table, df)
        report.compute()
        report.save()
        report.print_summary()
        reports[table] = report
    return reports


if __name__ == "__main__":
    run_quality_checks()