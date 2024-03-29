from typing import Optional, Union
from collections import UserDict


class Docs(UserDict):
    
    def __init__(self, description: Optional[str] = None, parameters_description: Optional[str] = None):
        super().__init__()
        self.description = description
        self.parameters_description = parameters_description
        
    def add_parameter_table_row(
            self,
            name: str,
            title: str,
            description: str,
            typ: Optional[Union[str, list[str]]] = None,
            example: Optional[Union[str, list[str]]] = None
    ):
        if typ is None:
            typ = "Any"

        if isinstance(typ, list):
            typ = ' | '.join(typ)

        if isinstance(example, list):
            example = ' | '.join(example)

        parameter_table_row = {
            "title": title,
            "description": description,
            "type": typ,
            "example": example
        }
        
        self[name] = parameter_table_row
    
