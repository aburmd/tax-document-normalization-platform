# tax-document-normalization-platform

Lambda parsers, canonical schemas, source mappings, and application logic for the Tax Document Normalization Platform.

> **No sensitive documents are stored in this repo.** Sample PDFs and real data live in a local helper workspace and are uploaded to S3 (encrypted, versioned, SSL-enforced).

## Structure

```
lambda/pdf_ingestion/
├── handler.py              # Lambda entry point
├── parsers/
│   ├── parser_router.py    # Routes to broker-specific parser
│   ├── base_parser.py      # Abstract base class
│   ├── schwab_parser.py    # Schwab PDF parser
│   ├── fidelity_parser.py  # Fidelity PDF parser
│   └── rsu_parser.py       # RSU event parser
├── common/
│   ├── s3_utils.py         # S3 download/upload
│   ├── checksum_utils.py   # SHA-256 dedup
│   ├── schema_validator.py # Canonical schema validation
│   └── mapping_loader.py   # Source→canonical mapping loader
└── requirements.txt

schemas/
├── canonical/              # 5 JSON schemas (doc metadata, txn, position, transfer, RSU)
└── source/                 # Broker-specific field mappings (schwab, fidelity, rsu)

docs/
├── architecture.md
└── data-model.md

tests/
├── unit/
└── integration/
```

## Setup

```bash
cd lambda/pdf_ingestion
pip install -r requirements.txt
```

## Run Tests

```bash
pip install pytest
pytest tests/unit/ -v
```

## Related Repo

CDK infrastructure: [tax-document-normalization-platform-cdk](https://github.com/aburmd/tax-document-normalization-platform-cdk)
