"""Export endpoints — generate annotated scope letter documents."""

import os
import logging

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response

from ..auth import User, require_active_user, require_role
from ..dependencies import get_db
from ..services.doc_export import ScopeDocGenerator


router = APIRouter()
log    = logging.getLogger(__name__)

TEMPLATE_PATH = os.getenv(
    "SCOPE_TEMPLATE_PATH",
    r"\\10.0.15.1\IT_Services\Services\Tyler\EstimateServices\Resources\TEMPLATE\TEMPLATE SCOPE LETTER.docx",
)


@router.get("/session/{session_id}/scope-letter")
async def export_scope_letter(
        session_id: int,
        db   = Depends(get_db),
        user: User = Depends(require_active_user) ) -> Response:
    """Generate a highlighted scope letter .docx for a completed session."""

    generator = ScopeDocGenerator(db, TEMPLATE_PATH)

    try:
        docx_bytes, filename = generator.generate_session_doc(session_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="Scope letter template not found on server")

    except Exception as e:
        log.error(f"Export failed for session {session_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Document generation failed: {e}")

    return Response(
        content     = docx_bytes,
        media_type  = "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        headers     = {"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/session/{session_id}/scope-letter-data")
async def get_scope_letter_data(
        session_id: int,
        db   = Depends(get_db),
        user: User = Depends(require_active_user) ) -> dict:
    """Structured scope letter data for the browser editor."""

    generator = ScopeDocGenerator(db, TEMPLATE_PATH)

    try:
        data = generator.build_editor_data(session_id)

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="Scope letter template not found on server")

    except Exception as e:
        log.error(f"Editor data failed for session {session_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to build editor data: {e}")

    return data


@router.get("/verify-template")
async def export_verification_doc(
        db   = Depends(get_db),
        user: User = Depends(require_role("admin")) ) -> Response:
    """Verify template mapping positions. Admin only."""

    generator = ScopeDocGenerator(db, TEMPLATE_PATH)

    try:
        docx_bytes = generator.generate_verification_doc()
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="Scope letter template not found on server")

    except Exception as e:
        log.error(f"Verification doc failed: {e}")
        raise HTTPException(status_code=500, detail=f"Document generation failed: {e}")

    return Response(
        content     = docx_bytes,
        media_type  = "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        headers     = {"Content-Disposition": 'attachment; filename="Template_Mapping_Verification.docx"'},
    )
