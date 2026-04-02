# FrostScan

FrostScan - веб-система мониторинга деградации многолетней мерзлоты на сельскохозяйственных территориях.
Проект объединяет:

- карту с полигонами риска и фильтрами;
- ML-пайплайн оценки риска по мультиспектральным растрам;
- API для запуска анализа, получения статистики и экспорта GeoJSON.

## Что умеет система

- Строит зоны риска деградации по годам и территориям.
- Показывает динамику, сводные метрики и карточку выбранного участка.
- Экспортирует результаты анализа в GeoJSON.
- Автоматически поднимает seed-данные, если внешнего датасета нет.

## Быстрый старт

### 1) Docker (рекомендуется)

```bash
docker compose up --build
```

Откройте:

- UI: `http://localhost:8000`
- Swagger API: `http://localhost:8000/docs`

### 2) Локальный запуск (Windows PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# Опционально: путь к внешнему датасету (по умолчанию D:/data)
$env:FROSTSCAN_DATA_DIR = "D:\data"

uvicorn backend.app.main:app --reload --host 0.0.0.0 --port 8000
```

Альтернатива:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_dev.ps1
```

## Режимы данных

FrostScan работает в двух режимах:

1. Внешний датасет (`FROSTSCAN_DATA_DIR`, по умолчанию `D:/data`):
- ищутся валидные `.tif` (>= 4 каналов, с CRS и transform);
- год извлекается из имени файла (например, `KANOPUS_2023...`);
- границы территорий собираются из `.shp`, если в путях встречаются `amga`/`yunkor`.

2. Seed-режим:
- если внешние данные не найдены, генерируются демо-границы, `parcels.csv` и растры за 2018-2025;
- используется для быстрого старта и демонстрации.

Важно:

- загрузка пользовательских файлов через `/upload/*` отключена в коде;
- активные пути данных хранятся в SQLite (`frostscan.db`, таблица `settings`).

## Основные API endpoint'ы

| Endpoint | Метод | Назначение |
|---|---|---|
| `/health` | `GET` | Статус сервиса, БД, seed и модели |
| `/analysis/run` | `POST` | Запуск анализа |
| `/analysis/results` | `GET` | Список всех запусков |
| `/analysis/{analysis_id}` | `GET` | Детали запуска и детекции |
| `/map/layers` | `GET` | GeoJSON-слои карты + фильтры |
| `/stats/summary` | `GET` | Сводные метрики по анализу |
| `/export/geojson` | `GET` | Скачивание GeoJSON результата |

Пример запуска анализа:

```bash
curl -X POST http://localhost:8000/analysis/run \
  -H "Content-Type: application/json" \
  -d "{\"territories\": [\"Amga\", \"Yunkor\"], \"years\": [2021, 2022, 2023], \"force_retrain\": false}"
```

Пример получения слоёв карты:

```bash
curl "http://localhost:8000/map/layers?territory=Amga&year=2023"
```

## ML и офлайн-скрипты

Переобучение модели:

```bash
python ml/train.py --force
```

Офлайн-предсказание и генерация артефактов:

```bash
python ml/predict.py --years 2018 2019 2020 2021 2022 2023 2024 2025
```

Выходные файлы:

- модель: `models/risk_model.joblib`
- GeoJSON: `outputs/geojson/*.geojson`
- растр риска: `outputs/rasters/*.tif`
- сводка: `outputs/reports/predict_summary.json`

## Структура проекта

```text
backend/
  app/
    routers/       # REST API (analysis, map, stats, export, health)
    services/      # бизнес-логика, работа с данными и пайплайном
    database.py    # SQLite-слой
frontend/
  index.html       # SPA-интерфейс
  map.html         # экран карты
  assets/          # JS/CSS/изображения
ml/
  train.py         # обучение модели
  predict.py       # пайплайн предсказания
data/
  seed/            # авто-генерируемые данные для демо
  uploads/         # внутренние служебные файлы/производные данные
outputs/           # результаты анализов
models/            # сохранённые ML-модели
```

## Технологии

- Backend: FastAPI, Pydantic, SQLite
- Geo: GeoPandas, Rasterio, Shapely, Fiona, PyProj
- ML: scikit-learn, NumPy, Pandas, SciPy
- Frontend: HTML/CSS/Vanilla JS + Leaflet
- Infra: Docker, Uvicorn

## Ограничения текущего MVP

- Нет авторизации и ролевой модели.
- Нет очередей/воркеров: анализ запускается синхронно в процессе API.
- Нет автотестов и CI-пайплайна.
- Upload API присутствует в коде, но сценарий пользовательской загрузки отключён.
