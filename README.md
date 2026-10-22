# RiskPulse: real-time financial risk analytics MVP

Synthetic stock prices and trades flow through **Apache Kafka → Spark Structured Streaming → Python Monte Carlo risk engine → live dashboard**. A Power BI bridge publishes the same snapshots to a live Power BI semantic model.

## Run locally

Requirements: Docker Desktop with at least 4 GB of memory available to Docker. The first start downloads images and Spark's Kafka connector, so it can take a few minutes.

```bash
docker compose up --build
```

Open **http://localhost:8080**. The dashboard begins showing VaR and CVaR after 11 price observations per held symbol, usually within about 20 seconds after Spark starts. It refreshes every 3 seconds.

If you open `dashboard/index.html` directly as a `file://` page, it now runs a clearly labeled browser-only simulation preview with live values and a chart. That preview does not connect to Kafka or Spark; use the localhost address above for the full pipeline.

The producer emits one observation per symbol per second and occasional buy/sell trades. Initial holdings are set in `risk_engine/config.py`. Spark reads JSON from the `trades` Kafka topic in 5-second micro-batches. A checkpoint tracks Kafka offsets; the SQLite sink deduplicates `(topic, partition, offset)` so replay does not double count positions. Data lives in `data/risk.db` and checkpoints in `checkpoints/`.

Stop with `Ctrl+C` and `docker compose down`. To start a fresh simulation, remove `data/risk.db*` and `checkpoints/` while the stack is stopped. Do not remove only one of these: the database and checkpoint represent the same stream history.

## Risk calculation

For each active position, the engine uses the latest **60 price ticks** and calculates log returns. It estimates the mean and covariance of those returns, draws **5,000 correlated multivariate-normal scenarios**, reprices the current holdings, and computes:

- **VaR 95%:** the 95th percentile of one-tick portfolio loss, floored at zero.
- **CVaR 95%:** the average loss among scenarios at or beyond that VaR threshold, floored at zero.

These are **one simulated tick** estimates, not one-day regulatory VaR. Prices are synthetic; estimates are for demonstration and have no market calibration, transaction costs, or stress events.

Set `MC_SCENARIOS`, `RISK_LOOKBACK_TICKS`, or `PRODUCER_INTERVAL_SECONDS` in a `.env` file to change the demo. If you change the lookback, restart the Spark service. A new stream history requires clearing both the database and checkpoint.

## Power BI live dashboard

This step needs a Power BI account and a Microsoft Entra app registration. The local dashboard runs without them.

1. In Microsoft Entra, register an app for your tenant, enable **public client flows**, and grant the delegated **Power BI Service → Dataset.ReadWrite.All** permission. Your tenant may require admin consent or Power BI API access settings.
2. In a second terminal on the host, install Python dependencies and run the bridge:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   export POWERBI_CLIENT_ID="YOUR_APP_CLIENT_ID"
   export POWERBI_TENANT_ID="YOUR_TENANT_ID"
   python -m risk_engine.powerbi_bridge
   ```

3. Follow the device-login URL and code printed in the terminal. The bridge creates **RiskPulse Live Risk** in **My workspace** and pushes new snapshots every 5 seconds. The model ID and authentication cache are stored in `data/` for restart.
4. In the Power BI service, open **My workspace → RiskPulse Live Risk → Create report**. Add three cards: latest `PortfolioValue`, `VaR95`, and `CVaR95`; add a line chart with `CalculatedAt` on the x-axis and `VaR95` and `CVaR95` as values. Pin the visuals individually to a new dashboard named **RiskPulse Live**. Pinned tiles from a push model update as new rows arrive. For latest-value cards, use a visual level filter on the latest `CalculatedAt` or a table visual sorted descending by `CalculatedAt`; a plain sum would aggregate history.

The Power BI bridge uses a **PushStreaming** semantic model, so historical report visuals and live dashboard tiles are both available. The bridge sends rows in batches and retries HTTP failures. Since the cursor is saved after a successful push, a crash between a successful push and cursor write could duplicate a few rows in Power BI; the source SQLite snapshots remain unique. Microsoft currently says creation of new real-time semantic models remains enabled through **October 31, 2027**. This Power BI integration therefore has a finite platform life; the local dashboard does not depend on it.

## Project structure

```text
risk_engine/producer.py       simulated prices and trades → Kafka
risk_engine/stream.py         Kafka → Spark Structured Streaming micro-batches
risk_engine/risk.py           Monte Carlo VaR/CVaR
risk_engine/store.py          idempotent SQLite sink
risk_engine/powerbi_bridge.py SQLite → Power BI PushStreaming API
dashboard/                    local live dashboard and API
```

## Quick checks

```bash
docker compose ps
curl http://localhost:8080/api/risk
docker compose logs --tail=30 spark
```

The API response should contain an increasing list of risk snapshots. The latest snapshot includes portfolio value, VaR, CVaR, current positions, and the number of scenario draws.

### Sources

- [Spark Kafka Structured Streaming integration](https://spark.apache.org/docs/3.5.5/structured-streaming-kafka-integration.html)
- [Power BI real-time streaming semantic models](https://learn.microsoft.com/en-us/power-bi/connect-data/service-real-time-streaming)
- [Power BI create push dataset API](https://learn.microsoft.com/en-us/rest/api/power-bi/push-datasets/datasets-post-dataset)
- [Power BI post rows API](https://learn.microsoft.com/en-us/rest/api/power-bi/push-datasets/datasets-post-rows)
