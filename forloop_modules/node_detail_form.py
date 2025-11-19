import ast

import forloop_modules.flog as flog
from forloop_modules.globals.local_variable_handler import File

class NodeParams(dict):
    """An object containing all necessary information about the item detail form backend - used for storage of the information
    Important!: It should always have dictionary representation
    
    Node: NewVariable
    Example of params_dict: {"variable_name": {"variable": null, "value": "link_id"}, "variable_value": {"variable": null, "value": "1"}}
    Example of params: {"variable_name": "link_id", "variable_value": "1"}
    
    """


        
    def code_repr(self,key=None):
        #to be implemented -> should return code representation both from value and variables
        
        final_dict={}
        for param_name in self.keys():
            # TODO: Detect that lists or dicts are not empty (but len() does not work with booleans)
            if isinstance(self[param_name]['value'], bool) or self[param_name]['value'] is not None:
                if self[param_name]['variable'] is not None:  
                    final_dict[param_name]={"code":self[param_name]['variable'],"is_variable":True}
                else:
                    final_dict[param_name]={"code":self[param_name]['value'],"is_variable":False}
            # hotfix to add ionput dataframe name as well
            elif self[param_name]['variable'] is not None:
                final_dict[param_name]={"code": self[param_name]['variable'],"is_variable":True}
            else:
                final_dict[param_name]={"code": "missing_"+str(param_name), "is_variable":True}

        result=self._get_repr_from_key(final_dict,key)
        return(result)
        
    def value_repr(self,key=None):
        #to be implemented -> should return value both from value and variables
        final_dict={}
        for param_name in self.keys():
            if self[param_name]['value'] is not None:
                final_dict[param_name]=self[param_name]['variable'] or self[param_name]['value']  
        
        result=self._get_repr_from_key(final_dict,key)
        return(result)
        
    def params_dict_repr(self,key=None):
        
        final_dict=dict(self)
        result=self._get_repr_from_key(final_dict,key)
        
        return(result)
        
    def _get_repr_from_key(self,final_dict,key=None):
        if key is None:
            return(final_dict)
        else:
            return(final_dict.get(key))
        


class NodeField(dict):
    """
    required keys: name, value
    is_internal: True (default)
    is_variable: False (default)
    category: None (default) - might have special behaviour based on this - e.g. show in data grid view
    
    #Template #1
    {"name":"input_df", "value":"df_A3", "category":"shown_df" "is_variable":True, "is_internal":True}
    
    #Template #2
    {"name":"stage", "value":"gamma", "category":"stage", "is_variable":False, "is_internal":True}
    
    #Template #3 -> future possible replacement of params
    {"name":"url", "value":"www.immobilier.ch", "category":None, "is_variable":False, "is_internal":False}
    """
    @classmethod
    def field_init(cls, name, value, category=None, is_variable:bool=False, is_internal:bool=True):
        new_node_field=cls({"name":name,"value":value,"category":category,"is_variable":is_variable,"is_internal":is_internal})
        return(new_node_field)
    
   
    
    
class NodeDetailForm:
    """Backend counterpart of ItemDetailForm - it comprises of two main things -> 1) node_params - specifying the current state of the form items + 2) typ/form_dict_list (fdl) specifying the template for the stored information"""
    def __init__(self, node_params = None, typ = None, pos = None, visible = True, form_dict_list = None, fields = None, node_uid = None): #TODO: fix inputs based on the final usage
        if node_params is None:    
            self.node_params=NodeParams()
        else:
            self.node_params = NodeParams(node_params) if type(node_params) == dict else node_params
        self.typ = typ
        if form_dict_list is not None: #TODO Dominik: maybe possible to deprecate whole if? self.form_dict_list defined later
            self.form_dict_list = form_dict_list
        else:
            self.form_dict_list = self.get_form_dict_list_from_typ(self.typ)
        self.fields=fields      # list of NodeField instances
        self.node_uid=node_uid

            
    def get_form_dict_list_from_typ(self,typ):
        """TODO!"""
        return []
        # TODO: is it really necessary to have the if statement?
    def define_form_dict_list(self,form_dict_list_options:dict={}, pipeline_function_handler_dict={}, image_component=None): #image_component to be deprecated when no pfh handler uses it -> replace by node_uid
        handler = pipeline_function_handler_dict.get(self.typ)
        if handler is not None:
            # Flatten the options structure for backward/forward compatibility
            raw_opts = form_dict_list_options or {}
            opts = raw_opts.get("options", raw_opts)
            
            try:
                # Attempt to call with the 'options' keyword for newer, dynamic handlers
                self.form_dict_list = handler.make_form_dict_list(options=opts, node_detail_form=self)
            except TypeError:
                # Fallback for legacy handlers that don't accept the 'options' keyword which was introduced in DBHandler repair I.
                self.form_dict_list = handler.make_form_dict_list(node_detail_form=self)

            ##### 16.10.2024 - Replaced by new form_dict_list mechanisms #####
            #try: #Try Except clause because some handlers might not have node_detail_form as argument of make_form_dict_list yet
            #    if len(form_dict_list_options)==0:
            #        form_dict_list_options={"node_detail_form":self}  #Temporary -> this should be later used everywhere and get rid of image_component
            #    else:
            #        form_dict_list_options["node_detail_form"]=self #if options are not empty, append it to the dictionary (e.g. LoadExcel)
            #    self.form_dict_list = handler.make_form_dict_list(image_component,**form_dict_list_options) #this works little bit cyclically but it works! (node_detail_form sent to make_form_dict_list which then changes form_dict_list of the node (self))
            #except TypeError as e: #if make_form_dict_list doesn't have node_detail_form argument
            #    flog.error(e)
            #    form_dict_list_options.pop("node_detail_form")
            #    self.form_dict_list = handler.make_form_dict_list(image_component,**form_dict_list_options)  #!!!!
            ##### 16.10.2024 - Replaced by new form_dict_list mechanisms #####

        else:
            self.form_dict_list=[]
        #else:
        #    form_dict_list = [{'Label': 'Not implemented correctly yet!'}]
        return self.form_dict_list
        
    # Deprecated with merge conflict
    # def get_chosen_value_by_name(self,name,variable_handler=None, return_value=False): #maybe deprecate variable handler when all handlers refactored
    #     if name in self.node_params:    
    #         result=self.node_params[name]["variable"] or self.node_params[name]["value"] 
    #         if return_value:
    #             result = variable_handler.variables[self.node_params[name]["variable"]].value


    def assign_value_by_name(self, name, value):
        params_dict = self.node_params.params_dict_repr()
        params_dict[name] = {'variable': None, 'value': value}

        return params_dict

    def get_chosen_value_by_name(self, name, variable_handler=None): #maybe deprecate variable handler when all handlers refactored
        result=None
        if name in self.node_params:
            if variable_handler is not None:
                # Try to load value from variable explorer
                variable_name = self.node_params[name]["variable"]
                if variable_name:
                    try:
                        # Use get_local_variable_by_name to properly handle Redis-stored variables (e.g., DataFrames)
                        local_variable = variable_handler.get_local_variable_by_name(variable_name)
                        if local_variable is not None:
                            result = local_variable.value
                            if isinstance(result, File):
                                result = result.file_name + "." + result.suffix
                        else:
                            # Variable not found, fall back to node params value
                            result = self.node_params[name]["value"]
                    except Exception as e:
                        flog.warning(f"Error retrieving variable '{variable_name}': {e}")
                        # Load from node params on error
                        result = self.node_params[name]["value"]
                else:
                    # No variable name, use direct value
                    result = self.node_params[name]["value"]
            else:
                result = self.node_params[name]["value"]
        else:
            flog.warning(f"{name} not in node_params")

        return(result)
        
    # Used mainly for code export purposes
    # TODO Daniel: Deprecate when all handlers have reasonable input_execute and code export from input_execute works well
    def get_variable_name_or_input_value_by_element_name(self, name:str, is_input_variable_name:bool=False):
        """Gets an input from an element (Entry, Combobox etc.) identified by it's name. Use mainly in code export.
        
        For regular cases, leave 'is_input_variable_name=False' ==> then the function works the same as 
        `get_chosen_value_by_name`, i.e. if there's a variable (FL variable rect) as input, take it's value else take input
        (str value returned as '" + value + "').
        
        For cases when we expect the input to be a variable name (new_variable_name etc.), use `is_input_variable_name=True`.
        The value is returned without quotes (variable name in code should not be enclosed by quotes)!

        Args:
            name (str): Name of the node_detail_form element
            is_input_variable_name (bool, optional): Defines whether the element expects a variable name or general input. Defaults to False.
        """        
        
        def _handle_non_variable_input(name, is_input_variable_name:bool):
            value = self.node_params[name]["value"]
            
            if is_input_variable_name or type(value) != str:
                """
                is_input_variable_name == True ==> Variable name --> return the entered string unchanged (without 
                additional quotes or anything)
                
                type(value) != str ==> Comboboxes and Comboentries return lists --> return the list without evaluating it
                """
                return value
            
            try:
                result = ast.literal_eval(value)
            except Exception as e:
                flog.warning(e)
                result = '"' + value + '"'
                
            return result
        
        assert name in self.node_params, f"{name} not in node_params"
        
        # variable rect is entered ==> return variable name
        if self.node_params[name]["variable"]:
            entered_variable_name = self.node_params[name]["variable"]
            return entered_variable_name
        
        # In case of non-variable input
        result = _handle_non_variable_input(name, is_input_variable_name)

        return result

    def get_first_field_value_by_category(self,field_category):
        """e.g. returns variable_name for dataframes
        used e.g. in gui_event_handler, gui_layout_context
        """
        fields = [x for x in self.fields if x["category"] == field_category]
        if len(fields) > 0:
            field_value = fields[0]["value"]
        else:
            field_value = None
        return field_value

    
    def find_show_info_element(self):
        show_info_elements=[]
        for i,row in enumerate(self.form_dict_list):
            element_type="Entry"
            chosen_element=row.get(element_type)

            # Element type is never Combobox Here!
            if chosen_element is None:
                element_type="Combobox"
                chosen_element=row.get(element_type)
            if chosen_element is None:
                element_type="ComboEntry"
                chosen_element=row.get(element_type)
                
                
            if chosen_element is not None:
                show_info=chosen_element.get("show_info")
                if show_info:
                    show_info_elements.append(row[element_type]["name"])
                    
          
        return(show_info_elements)
                    
        
        
    def refresh_show_info(self,typ): #typ could potentially be updated from node_detail_form - but be careful as it can currently be None
        node_typ_show_info_lambda_dict={
            "LoadExcel":lambda name_value_dict:list(name_value_dict.values())[0].split("/")[-1],
            "LoadWebsite": lambda name_value_dict:list(name_value_dict.values())[0].replace("https://","").replace("http://","").replace("www.",""),
            "ConvertVariableType": lambda name_value_dict:name_value_dict["variable_name"]+" --> "+name_value_dict["variable_type"],
            "NewVariable": lambda name_value_dict:name_value_dict["variable_name"]+"="+name_value_dict["variable_value"],
            "Wait": lambda name_value_dict:name_value_dict["milliseconds"]+" milliseconds",
            "DataFrame": lambda _: self.get_first_field_value_by_category("df_variable_name"),
            "DictToDf": lambda _: self.get_first_field_value_by_category("df_variable_name")
            } #TODO Dominik: to be moved to pipeline function handlers - but from GLC it shouldnt be imported (cyclical imports)
        
    
        show_info_elements=self.find_show_info_element()
        show_info_name_value_dict={}
        for i, element_name in enumerate(show_info_elements):
            element_value=self.get_chosen_value_by_name(element_name)
            if element_value is not None:
                show_info_name_value_dict[element_name]=self.get_chosen_value_by_name(element_name)
        
            
        if typ in node_typ_show_info_lambda_dict:
            try:
                lambda_result = node_typ_show_info_lambda_dict[typ](show_info_name_value_dict)
                info_text = lambda_result if lambda_result is not None else ""
            except Exception as e:
                info_text=""
                flog.warning(e,self)
        else:
            if len(show_info_name_value_dict)>0:
                info_text=str(list(show_info_name_value_dict.values())[0])
            else:
                info_text=""
        return info_text
    
    def to_json_format(self, key=None):
        node_detail_form_dict={}
        node_detail_form_dict["params"]=NodeParams()
        node_detail_form_dict["fields"]=[]
        
        for i,field in enumerate(self.fields):
            node_detail_form_dict["fields"].append(field)

        #TODO Params to json missing

        if key is not None:
            node_detail_form_dict = node_detail_form_dict[key]
        
        return node_detail_form_dict
