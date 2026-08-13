from fastapi import APIRouter, Depends
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.services.models_service import get_models_surface_status
from data_ingestion_toolbox.models import ModelSurfaceStatusResponse

router = APIRouter(prefix="/api/models", tags=["models"])


@router.get("/status", response_model=ModelSurfaceStatusResponse)
def get_models_status(
    db: Session = Depends(get_db_session_dep),
) -> ModelSurfaceStatusResponse:
    try:
        return get_models_surface_status(db)
    except SQLAlchemyError as exc:
        raise db_service_unavailable(exc) from exc
