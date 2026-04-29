import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple
import pandas as pd
from contracts.schema_contracts import TableContract, ALL_CONTRACTS
from config.local_config import LOGS_DIR

logger = logging.getLogger("validator")


class ValidationResult:
    def init(self, table_name: str):
        self.table_name = table_name
        self.timestamp = datetime.now().isoformat()
        self.total_rows = 0
        self.violations: List[Dict] = []
        self.warnings: List[Dict] = []
        self.passed: List[str] = []

    def add_violation(self, check: str, details: str, affected_rows: int = 0):
        self.violations.append({
            "check": check,
            "details": details,
            "affected_rows": affected_rows,
            "severity": "ERROR"
        })

    def add_warning(self, check: str, details: str, affected_rows: int = 0):
        self.warnings.append({
            "check": check,
            "details": details,
            "affected_rows": affected_rows,
            "severity": "WARNING"
        })

    def add_passed(self, check: str):
        self.passed.append(check)

    @property
    def is_valid(self) -> bool:
        return len(self.violations) == 0

    @property
    def violation_count(self) -> int:
        return len(self.violations)

    def summary(self) -> str:
        status = "PASSED" if self.is_valid else "FAILED"
        return (
            f"[{status}] {self.table_name} | "
            f"Rows: {self.total_rows} | "
            f"Violations: {self.violation_count} | "
            f"Warnings: {len(self.warnings)} | "
            f"Passed: {len(self.passed)}"
        )

    def to_dict(self) -> Dict:
        return {
            "table_name": self.table_name,
            "timestamp": self.timestamp,
            "total_rows": self.total_rows,
            "is_valid": self.is_valid,
            "violations": self.violations,
            "warnings": self.warnings,
            "passed_checks": self.passed
        }


class ContractValidator:
    def init(self, contract: TableContract):
        self.contract = contract
        self.log_path = os.path.join(LOGS_DIR, "contract_violations.log")

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        result = ValidationResult(self.contract.table_name)
        result.total_rows = len(df)

        self._check_required_columns(df, result)
        self._check_nulls(df, result)
        self._check_value_ranges(df, result)
        self._check_allowed_values(df, result)
        self._check_primary_key(df, result)
        self._check_duplicates(df, result)

        self._log_result(result)
        return result

    def _check_required_columns(self, df: pd.DataFrame, result: ValidationResult):
        required = self.contract.get_required_columns()
        missing = [col for col in required if col not in df.columns]
        if missing:
            result.add_violation(
                "required_columns",
                f"Missing required columns: {missing}",
                affected_rows=len(df)
            )
        else:
            result.add_passed("required_columns")

    def _check_nulls(self, df: pd.DataFrame, result: ValidationResult):
        for col_contract in self.contract.columns:
            if col_contract.name not in df.columns:
                continue
            if not col_contract.nullable:
                null_count = df[col_contract.name].isna().sum()
                if null_count > 0:
                    result.add_violation(
                        f"null_check_{col_contract.name}",
                        f"Column '{col_contract.name}' has {null_count} null values but is marked NOT NULL",
                        affected_rows=int(null_count)
                    )
                else:
                    result.add_passed(f"null_check_{col_contract.name}")
            else:
                null_pct = df[col_contract.name].isna().mean() * 100
                if null_pct > 80:
                    result.add_warning(
                        f"high_null_{col_contract.name}",
                        f"Column '{col_contract.name}' has {null_pct:.1f}% null values",
                        affected_rows=int(df[col_contract.name].isna().sum())
                    )

    def _check_value_ranges(self, df: pd.DataFrame, result: ValidationResult):
        for col_contract in self.contract.columns:
            if col_contract.name not in df.columns:
                continue
            col = df[col_contract.name].dropna()
            if col_contract.min_value is not None:
                violations = (col < col_contract.min_value).sum()
                if violations > 0:
                    result.add_violation(
                        f"min_value_{col_contract.name}",
                        f"Column '{col_contract.name}' has {violations} values below minimum {col_contract.min_value}",
                        affected_rows=int(violations)
                    )
                else:
                    result.add_passed(f"min_value_{col_contract.name}")
            if col_contract.max_value is not None:
                violations = (col > col_contract.max_value).sum()
                if violations > 0:
                    result.add_violation(
                        f"max_value_{col_contract.name}",
                        f"Column '{col_contract.name}' has {violations} values above maximum {col_contract.max_value}",
                        affected_rows=int(violations)
                    )
                else:
                    result.add_passed(f"max_value_{col_contract.name}")

    def _check_allowed_values(self, df: pd.DataFrame, result: ValidationResult):
        for col_contract in self.contract.columns:
            if col_contract.allowed_values is None:
                continue
            if col_contract.name not in df.columns:
                continue
            col = df[col_contract.name].dropna()
            invalid = ~col.isin(col_contract.allowed_values)
            violation_count = invalid.sum()
            if violation_count > 0:
                invalid_vals = col[invalid].unique().tolist()[:5]
                result.add_violation(
                    f"allowed_values_{col_contract.name}",
                    f"Column '{col_contract.name}' has {violation_count} invalid values. Examples: {invalid_vals}",
                    affected_rows=int(violation_count)
                )
            else:
                result.add_passed(f"allowed_values_{col_contract.name}")

    def _check_primary_key(self, df: pd.DataFrame, result: ValidationResult):
        if not self.contract.primary_key:
            return
        pk_cols = [col for col in self.contract.primary_key if col in df.columns]
        if not pk_cols:
            return
        duplicates = df.duplicated(subset=pk_cols).sum()
        if duplicates > 0:
            result.add_violation(
                "primary_key_uniqueness",
                f"Primary key {pk_cols} has {duplicates} duplicate rows",
                affected_rows=int(duplicates)
            )
        else:
            result.add_passed("primary_key_uniqueness")

    def _check_duplicates(self, df: pd.DataFrame, result: ValidationResult):
        total_dups = df.duplicated().sum()
        if total_dups > 0:
            result.add_warning(
                "duplicate_rows",
                f"Found {total_dups} completely duplicate rows",
                affected_rows=int(total_dups)
            )
    def _log_result(self, result: ValidationResult):
        log_line = json.dumps(result.to_dict()) + "\n"
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(log_line)
        if result.is_valid:
            logger.info(result.summary())
        else:
            logger.warning(result.summary())


def validate_all_tables(data_dir: str) -> Dict[str, ValidationResult]:
    import os
    results = {}
    for table_name, contract in ALL_CONTRACTS.items():
        csv_path = os.path.join(data_dir, f"sample_{table_name}.csv")
        if not os.path.exists(csv_path):
            continue
        df = pd.read_csv(csv_path)
        validator = ContractValidator(contract)
        result = validator.validate(df)
        results[table_name] = result
        print(result.summary())
    return results


if __name__ == "__main__":
    from config.local_config import DATA_DIR
    logging.basicConfig(level=logging.INFO)
    results = validate_all_tables(DATA_DIR)
    total_violations = sum(r.violation_count for r in results.values())
    print(f"\nTotal violations across all tables: {total_violations}")