from typing import Optional
from collections import UserDict

class Docs(UserDict):
    
    def __init__(self, description: Optional[str] = None, parameters_description: Optional[str] = None):
        super().__init__()
        self.description = description
        self.parameters_description = parameters_description
        
    def add(self, name: str, title: str, description: str, typ: Optional[str] = None, example: Optional[str] = None):
        if typ is None:
            typ = "Any"
            
        if example is None:
            example = "Will be added soon..."
            
        parameter_table_row = {
            "title": title,
            "description": description,
            "type": typ,
            "example": example
        }
        
        self[name] = parameter_table_row
    
