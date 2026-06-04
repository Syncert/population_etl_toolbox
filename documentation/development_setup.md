# Development Setup

## Python package install

```bash
pip install -e .
python -c "import population_etl_toolbox"
```

## API local run

```bash
pip install -e .[api,dev]
uvicorn app.main:app --app-dir apps/api/src --reload --port 8000
```

## Web local run

```bash
cd apps/web
npm install
npm run lint
npm run build
```
