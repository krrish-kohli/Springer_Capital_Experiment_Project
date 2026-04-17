## ClickHouse (demo)

### What’s here
- `ddl/`: schemas for raw/silver and audit tables
- `seed/`: sample inserts with intentional silver issues (duplicates, missing keys, suspicious defaults)

### Apply DDL + seed

```bash
./scripts/init_clickhouse.sh
./scripts/seed_demo_data.sh
```

