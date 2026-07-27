from sqlalchemy import text
from sqlalchemy.orm import Session

from data_ingestion_toolbox.models import ModelSurfaceStatusResponse

_MODEL_RELATIONS = [
    "gold.fact_model_output",
    "gold.v_metric_forecast",
    "gold.v_scenario_result",
]


def get_models_surface_status(db: Session) -> ModelSurfaceStatusResponse:
    found_relations: list[str] = []

    for relation_name in _MODEL_RELATIONS:
        exists = db.execute(
            text("SELECT to_regclass(:relation_name) IS NOT NULL"),
            {"relation_name": relation_name},
        ).scalar()
        if bool(exists):
            found_relations.append(relation_name)

    if found_relations:
        details = "Model surfaces detected: " + ", ".join(found_relations)
        return ModelSurfaceStatusResponse(status="ready", models_enabled=True, details=details)

    details = "No model surface relations found; model endpoints are planned."
    return ModelSurfaceStatusResponse(status="planned", models_enabled=False, details=details)
