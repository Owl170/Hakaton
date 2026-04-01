# FrostScan

MVP web system for detecting permafrost degradation risks on agricultural land.
Dataset mode: fixed project dataset from `D:/data` (custom uploads disabled).

## Run (local)

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
# optional: point to real data root (default is D:/data)
set FROSTSCAN_DATA_DIR=D:\data
uvicorn backend.app.main:app --reload --host 0.0.0.0 --port 8000
```

Open `http://localhost:8000`.

## Run (docker)

```bash
docker compose up --build
```

Open `http://localhost:8000`.

## ML scripts

```bash
python ml/train.py --force
python ml/predict.py --years 2018 2019 2020 2021 2022 2023 2024 2025
```
