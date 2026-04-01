# Data Model

## Canonical Schemas

All brokerage data is normalized into 5 canonical schemas before storage. Schema definitions live in `schemas/canonical/`.

### 1. Document Metadata
Tracks each ingested document: source file, broker, tax year, checksum, parse status.

### 2. Transaction Event
Individual financial events: BUY, SELL, DIVIDEND, INTEREST, TRANSFER_IN, TRANSFER_OUT, RSU_VEST, RSU_SELL_TO_COVER, RSU_TRANSFER_TO_MARGIN, JOURNAL, TAX_WITHHOLDING, OTHER.

### 3. Position Snapshot
Point-in-time holdings: symbol, quantity, market value, cost basis, unrealized gain/loss.

### 4. Transfer
Asset movements between brokers/accounts. Non-taxable events that preserve cost basis.

### 5. RSU Event
RSU-specific: vest events, sell-to-cover, transfers to margin, withholdings.

## Source Mappings

Each broker has a mapping config in `schemas/source/` that defines:
- **source_columns** — Column headers as they appear in the broker's PDF tables
- **field_mappings** — Source column → canonical field name
- **event_type_mappings** — Broker-specific action labels → canonical event types

### Supported Brokers
| Broker | Mapping File |
|--------|-------------|
| Schwab | `schwab_mapping.json` |
| Fidelity | `fidelity_mapping.json` |
| RSU (Fidelity) | `rsu_mapping.json` |

## S3 Path Convention

```
s3://{bucket}/{zone}/{broker}/{account_type}/{tax_year}/{document_id}.{ext}
```
