# NYC Taxi Data-Ingestion Pipeline (AWS) - personal test version

> **This copy is for testing in your own AWS account.** It differs from the team version in three ways:
> it uses the public **AWS SDK for pandas managed layer** (no Artifactory), simple tags (no `config.yaml`),
> and reserved concurrency can be turned off (`RESERVED_CONCURRENCY=0`) for accounts with a low Lambda limit.
> The pipeline code, S3 layout, rules and tests are identical.
>
> ```bash
> OWNER=vinay ALERT_EMAIL=you@example.com \
> PANDAS_LAYER_ARN=arn:aws:lambda:<region>:336392948345:layer:AWSSDKPandas-Python312:<version> \
> ./scripts/deploy.sh
> OWNER=vinay ./scripts/smoke_test.sh      # end-to-end test; run twice to check re-runs
> OWNER=vinay ./scripts/teardown.sh        # remove everything when done
> ```

Turns one month of raw NYC Yellow Taxi trip data into clean, queryable tables in Amazon Athena.
Rejected rows are kept in `quarantine/` with a reason; nothing is silently deleted.

**Design:** see [`docs/technical_design.pdf`](docs/technical_design.pdf) · **Architecture:** [`docs/architecture.png`](docs/architecture.png)

## Repository layout

| Path | Purpose |
|---|---|
| `infra/resources.yaml` | CloudFormation stack 1: data bucket (deployed first; also holds code and layer zips under `artifacts/`) |
| `infra/template.yaml` | CloudFormation stack 2: EventBridge rule, Lambda (managed pandas layer), IAM role, Glue DB, Athena workgroup, alarm |
| `src/transform/handler.py` | Lambda entry point: reads the event, runs the pipeline steps |
| `src/transform/rules.py` | Cleaning rules and zone join (no AWS calls, unit-tested) |
| `tests/` | Unit tests (`pytest`) |
| `sql/sample_queries.sql` | Three sample Athena queries |
| `scripts/` | Deploy, upload and teardown helpers |
| `docs/` | Architecture diagram, design document, demo deck |

## Quick start

```bash
pip install -r requirements-dev.txt
pytest tests/ -v                 # unit tests
./scripts/deploy.sh              # package + deploy the stack
./scripts/upload_raw.sh          # upload zone lookup, then the trip file (triggers the run)
./scripts/teardown.sh            # empty the bucket and delete the stack
```

_Sections to complete as the build progresses: deploy/run details, trigger and transform choice, re-runnability, results, improvements, 100× scale, use of AI tools._
