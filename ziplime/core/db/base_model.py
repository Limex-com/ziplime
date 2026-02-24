from typing import Any

from sqlalchemy import orm, JSON
from sqlalchemy.orm import DeclarativeBase

from ziplime.utils.string_utils import camel_case_to_snake_case


class BaseModel(DeclarativeBase):
    __name__: str

    type_annotation_map = {
        dict[str, Any]: JSON
    }

    # Generate __tablename__ automatically
    @orm.declared_attr
    def __tablename__(cls) -> str:
        return camel_case_to_snake_case(cls.__name__).lower()
