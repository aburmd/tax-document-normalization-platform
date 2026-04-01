# Architecture

## Overview

The Tax Document Normalization Platform ingests brokerage PDF documents, parses them into structured data, validates against canonical schemas, and stores normalized output in S3.

## Flow

```
Upload PDF → S3 raw/ → S3 Event → Lambda → Parse → Validate → S3 cleansed/
                                                              → S3 audit/
                                                   (fail) → S3 rejected/
```

## Components

### Lambda Handler
Entry point receives S3 event, extracts metadata from the S3 key path (broker, account_type, tax_year), routes to the correct parser.

### Parser Framework
- **parser_router.py** — Routes to broker-specific parser based on metadata
- **base_parser.py** — Abstract base class with shared mapping logic
- **schwab_parser.py** / **fidelity_parser.py** / **rsu_parser.py** — Broker-specific PDF extraction

### Common Utilities
- **s3_utils.py** — Download/upload operations
- **checksum_utils.py** — SHA-256 dedup
- **schema_validator.py** — Validates output against canonical JSON schemas
- **mapping_loader.py** — Loads source→canonical field mappings

### Canonical Data Model
5 schemas enforced before any data reaches cleansed/:
1. Document Metadata
2. Transaction Event
3. Position Snapshot
4. Transfer
5. RSU Event

### S3 Zones

| Zone | Purpose |
|------|---------|
| raw/ | Uploaded source PDFs |
| cleansed/ | Validated canonical JSON + CSV |
| rejected/ | Failed parse error payloads |
| audit/ | Parse manifests and lineage |
| config/ | Schema definitions, mapping configs |

## Infrastructure
See [tax-document-normalization-platform-cdk](https://github.com/aburmd/tax-document-normalization-platform-cdk) for CDK stack.
