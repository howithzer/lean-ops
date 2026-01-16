# Development Strategy - MLK Weekend Push

## Current State Assessment

### ✅ What We Have
| Area | Status |
|------|--------|
| **Modularization** | Glue job split into utils/ (config, flatten, schema_evolution) |
| **Logging** | Proper `logging` module with consistent format |
| **Build Scripts** | `build_glue.sh` for packaging |
| **Infrastructure** | Terraform with 76 resources |

### ⚠️ Gaps to Address
| Area | Current | Target |
|------|---------|--------|
| **Environment Variables** | Mostly hardcoded | Fully parameterized |
| **Unit Tests** | None | pytest + moto for each module |
| **Integration Tests** | Manual E2E | Automated with fixtures |
| **CI/CD** | None | GitHub Actions |
| **Feature Tracking** | None | DynamoDB table for pipeline status |

---

## Weekend Execution Plan

### Day 1 (Saturday) - Foundation
1. Add environment variable config class
2. Set up pytest with conftest.py
3. Write unit tests for flatten.py
4. Write unit tests for schema_evolution.py

### Day 2 (Sunday) - CI/CD
1. Create GitHub Actions workflow
2. Add terraform fmt/validate checks
3. Add linting with ruff
4. Create develop branch

### Day 3 (Monday) - Features
1. Implement status tracking table
2. Add status update Lambda
3. Integrate into Step Function
4. E2E test with status tracking

---

## Testing Checklist Template

For each new function, ensure:
- [ ] Docstring with Args/Returns
- [ ] Type hints
- [ ] Unit test in corresponding test_*.py
- [ ] Edge case tests (empty input, None, large data)
- [ ] Integration test if touching AWS services
- [ ] Update README if public API changes

---

## Branch Workflow

```
main           # Production-ready
├── develop    # Integration branch
├── feature/status-tracking
├── feature/semantic-layer
└── bugfix/*
```

## Commits Made Today

- `9d7dd05` docs: Update README with Glue build/deploy workflow
- `7560c29` feat: Production Glue deployment with extra-py-files
- `3a54503` refactor: Modularize Glue job into utils package
- `e2ba09a` feat: Deep JSON flatten (5 levels) with envelope exclusion
