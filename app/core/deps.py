from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy.orm import Session


def get_session(request: Request):
    with request.app.state.db.session() as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]

__all__ = ["SessionDep", "get_session"]
