import sys
if sys.platform!="linux" and sys.platform!="linux2":
    import pyautogui
    import doclick.doclick_core as dc
    import doclick.doclick_image as di
    import pynput

import forloop_modules.flog as flog
import forloop_modules.queries.node_context_requests_backend as ncrb

from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.globals.variable_handler import variable_handler 
from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList   

from forloop_modules.node_detail_form import NodeField
from forloop_modules.function_handlers.auxilliary.abstract_function_handler import AbstractFunctionHandler, Input

import forloop_modules.function_handlers.auxilliary.forloop_code_eval as fce

####### PROBLEMATIC IMPORTS TODO: REFACTOR #######
#from src.gui.item_detail_form import ItemDetailForm #independent on GLC - but is Frontend -> Should separate to two classes
####### PROBLEMATIC IMPORTS TODO: REFACTOR #######

sys.modules["mouse"]=None #mouse import forbidden - not compatible on mac


class ClickHandler(AbstractFunctionHandler):
    icon_type = "Click"
    fn_name = "Click"
    type_category = ntcm.categories.rpa

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("X:")
        fdl.entry(name="x", text="0", row=1, input_types=["int", "float"], show_info=True, required=True)
        fdl.label("Y:")
        fdl.entry(name="y", text="0", row=2, input_types=["int", "float"], show_info=True, required=True)
        fdl.label("Pipette current position (Press Enter Key)")
        fdl.label("(X,Y) = (0,0)")
        fdl.label("Double click")
        fdl.checkbox(name="double_click", bool_value=False, row=5)
        

        return fdl

    # def __new__(cls, x, y, *args, **kwargs):

    def execute(self, node_detail_form):
        x = node_detail_form.get_chosen_value_by_name("x", variable_handler)
        y = node_detail_form.get_chosen_value_by_name("y", variable_handler)
        double_click = node_detail_form.get_chosen_value_by_name("double_click", variable_handler)

        self.direct_execute(x, y, double_click)

    def direct_execute(self, x, y, double_click):
        if x == "":
            x = 0
        if y == "":
            y = 0
        
        x = int(x)
        y = int(y)
        
        clicks=1
        if double_click:
            clicks=2

        inp=Input()
        inp.assign("x",x)
        inp.assign("y",y)
        inp.assign("clicks",clicks)
        
        if sys.platform!="linux" and sys.platform!="linux2": #pyautogui not supported on linux
            self.input_execute(inp)
        else:
            flog.info("Clicking with Forloop is disabled on linux OS")

    def input_execute(self, inp):
        pyautogui.moveTo(inp("x"), inp("y"))
        pyautogui.click(x=inp("x"), y=inp("y"), clicks=inp("clicks"))

    def export_imports(self, *args):
        imports = ["import pyautogui"]
        return (imports)



class ClickImageHandler:  # TODO: AbstractFunctionHandler
    def __init__(self):
        self.icon_type = "ClickImage"
        self.fn_name = "Click Image"

        self.type_category = ntcm.categories.rpa

    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Image path:")
        fdl.entry(name="image_path", text="", row=1, input_types=["str"], required=True, )
        fdl.label("Click offset X")
        fdl.entry(name="offset_x", text="", row=2, input_types=["int", "float"])
        fdl.label("Click offset Y")
        fdl.entry(name="offset_y", text="", row=3, input_types=["int", "float"])

        return fdl

    def execute(self, node_detail_form):
        image_path = node_detail_form.get_chosen_value_by_name("image_path", variable_handler)
        offset_x = node_detail_form.get_chosen_value_by_name("offset_x", variable_handler)
        offset_y = node_detail_form.get_chosen_value_by_name("offset_y", variable_handler)

        self.direct_execute(image_path, offset_x, offset_y)

    def direct_execute(self, image_path, offset_x, offset_y):
        img = di.take_screenshot()
        subimg = di.load_img(image_path)
        true_points = di.detect_subimg(img, subimg)
        coor = true_points[0]

        x = offset_x + coor[0] if offset_x else 0
        y = offset_y + coor[0] if offset_y else 0

        command = f"Click({x}, {y})"
        
        if sys.platform!="linux" and sys.platform!="linux2": #doclick not supported on linux
            dc.execute_order(command)
        else:
            flog.info("Clicking with Forloop is disabled on linux OS")
        
        flog.info("Clicked")

    def export_code(self, node_detail_form):
        code = """
        #Not implemented
        """
        return (code)

    def export_imports(self, *args):
        imports = ["import doclick.doclick_core as dc", "import doclick.doclick_image as di"]
        return (imports)



class WriteHandler(AbstractFunctionHandler):  # TODO AbstractFunctionHandler
    def __init__(self):
        self.icon_type = "Write"
        self.fn_name = "Write"

        self.type_category = ntcm.categories.rpa
        
    def make_form_dict_list(self, *args, node_detail_form=None):

        fdl = FormDictList()
        fdl.label(self.fn_name)
        fdl.label("Text:")
        fdl.entry(name="text", text="", row=1, input_types=["str"], show_info=True)

        return fdl

    def execute(self, node_detail_form):
        text = node_detail_form.get_chosen_value_by_name("text", variable_handler)

        self.direct_execute(text)

    def execute_with_params(self, params):
        text = params["text"]

        self.direct_execute(text)
        
    def direct_execute(self,text):
        
        inp=Input()
        inp.assign("text",text)
         
        if sys.platform!="linux" and sys.platform!="linux2": #doclick not supported on linux
            self.input_execute(inp)
        else:
            flog.info("Clicking with Forloop is disabled on linux OS")
        
        
        #dc.execute_order("Write(" + text + ")")
        
    def input_execute(self,inp):
        dc.execute_order("Write("+inp("text",inside_str=True)+")")
    
    def export_imports(self, *args):
        imports = ["import doclick.doclick_core as dc"]
        return (imports)



class UseKeyHandler(AbstractFunctionHandler):

    def __init__(self):
        self.icon_type = 'UseKey'
        self.fn_name = 'Use Key'
        
        self.type_category = ntcm.categories.rpa

    def make_form_dict_list(self, *args, node_detail_form=None, **kwargs):
        
        options1 = ['Ctrl', 'cmd (Win)', 'Alt', 'Enter (Return)', 'Tab', 'Esc', 'CapsLock', 'BackSpace', 'Delete', 'Space', 'Shift','AltGR']
        options2 = ['Alt', 'Shift', 'Tab', 'cmd (Win)', 'F1', 'F2', 'F3', 'F4', 'F5']
        
        #if node_detail_form.fields
        
        fdl = FormDictList()
        fdl.label("Use Key (or Combination of Keys)")
        fdl.label("Key 1")
        fdl.comboentry(name="key1", text="Enter (Return)", options=options1, row=1)
        fdl.button(function=self.add_key, function_args=node_detail_form, text="Add key", enforce_required=False)
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)
        
        # for template init (without node_detail_form
        if node_detail_form is not None:
            stage_list=[x["value"] for x in node_detail_form.fields if x["name"]=="stage"]
        else:
            stage_list = []

        flog.info(f"STAGELIST: {stage_list}")
        if len(stage_list)>0:
            stage=stage_list[0]
            stage_id=int(stage.replace("alpha",""))
            for i in range(2,stage_id+1):
                fdl.insert(-2, {"Label":f"Key {i}", "ComboEntry":{"name":f"key{i}","text": "", "options":options2}})
        
        
        return fdl

    def add_key(self, node_detail_form):
        form_dict_list=node_detail_form.form_dict_list

        num_of_key_entries = len(form_dict_list) - 3 # "form_dict_list element count" - 1 Label and 2 Buttons
        new_stage_id=num_of_key_entries+1 #first stage_id was 1, new_stage_id = 2 after first call of add_key
        
        if num_of_key_entries < 3:
            #form_dict_list.insert(-2, {"Label":f"Key {num_of_key_entries + 1}", "ComboEntry":{"name":f"key{num_of_key_entries + 1}","text": "", "options":options2}})
            
            ncrb.update_node_by_uid(node_detail_form.node_uid, fields=[NodeField.field_init("stage", "alpha"+str(new_stage_id),"stage")])

            #image.item_detail_form.elements = []
            #image.item_detail_form = ItemDetailForm(form_dict_list, self.icon_type, magnetic = False)
            #image.item_detail_form.generate_elements()
        else:
            ncrb.new_popup([500, 400], "UseKeyWarningPopup", {})

    def execute(self, node_detail_form):
        key1 = node_detail_form.get_chosen_value_by_name("key1", variable_handler)
        key2 = None
        key3 = None

        try:
            key2 = node_detail_form.get_chosen_value_by_name("key2", variable_handler)
        except Exception as e:
            flog.error(e)
        try:
            key3 = node_detail_form.get_chosen_value_by_name("key3", variable_handler)
        except Exception as e:
            flog.error(e)

         
        
        self.direct_execute(key1=key1, key2=key2, key3=key3)

    def direct_execute(self, *args, key1=None, key2=None, key3=None):
        
        #### Getting keys from params when running in pipeline
        #### TODO: Can this be done differently?
        if args:
            key1 = args[0][0]
            try:
                key2 = args[1][0]
            except:
                pass
            try:
                key3 = args[2][0]
            except:
                pass
        ############################################
        if sys.platform!="linux" and sys.platform!="linux2": #doclick not supported on linux

            
            keyboard = pynput.keyboard.Controller()
            key = pynput.keyboard.Key
    
            modifiers_and_keys = {'Ctrl': key.ctrl, 'cmd (Win)': key.cmd, 'Alt': key.alt, 'Enter (Return)': key.enter,
                                  'Esc': key.esc,
                                  'CapsLock': key.caps_lock, 'BackSpace': key.backspace, 'Delete': key.delete,
                                  'Space': key.space, 'Shift': key.shift,
                                  'AltGR': key.alt_gr, 'Tab': key.tab}
    
            f_key_numbers = range(1, 13) # F1, ..., F12
            for i in f_key_numbers:
                new_key = fce.eval_expression(f'key.f{i}', globals(), locals())
                if new_key:
                    modifiers_and_keys[f'F{i}'] = new_key
    
            key1, key2, key3 = self._append_modifiers_to_keys_if_possible(modifiers_and_keys, [key1, key2, key3])
    
            if key2 is None and key3 is None:
                try:
                    keyboard.press(key1)
                    keyboard.release(key1)
                    flog.info(f'{key1} PRESSED')
                except Exception as e:
                    flog.error(message=f'Incorrect input form: "{key1}", {e}')
            else:
                self._perform_keyboard_shortcut(keyboard, key1, key2, key3)
                
        else:
            flog.info("Clicking with Forloop is disabled on linux OS")

    def _append_modifiers_to_keys_if_possible(self, modifiers: dict, keys:list) -> tuple:
        
        for i, key in enumerate(keys):
            flog.info(f"KEY {i+1} ", key)
            if key in modifiers.keys():
                keys[i] = modifiers[key]
        
        return tuple(keys)  

    def _perform_keyboard_shortcut(self, keyboard, key1, key2, key3):

        if key3 is None:
            try:
                self._perform_two_key_shortcut(keyboard, key1, key2)
            except Exception as e:
                flog.error(message=f'Incorrect input form: "{key1}", {e}')
        else:
            try:
                self._perform_three_key_shortcut(keyboard, key1, key2, key3) 
            except Exception as e:
                flog.error(message=f'Incorrect input form: "{key1}", {e}')

    def _perform_two_key_shortcut(self, keyboard, key1, key2):

        with keyboard.pressed(key1):
            keyboard.press(key2)
            keyboard.release(key2)

    def _perform_three_key_shortcut(self, keyboard, key1, key2, key3):

        with keyboard.pressed(key1):
            with keyboard.pressed(key2):
                keyboard.press(key3)
                keyboard.release(key3)    

    def export_code(self, node_detail_form):

        modifiers_and_keys = {'Ctrl': 'key.ctrl', 'cmd (Win)': 'key.cmd', 'Alt': 'key.alt',
                              'Enter (Return)': 'key.enter', 'Esc': 'key.esc',
                              'CapsLock': 'key.caps_lock', 'BackSpace': 'key.backspace', 'Delete': 'key.delete',
                              'Space': 'key.space', 'Shift': 'key.shift',
                              'AltGR': 'key.alt_gr', 'Tab': 'key.tab', 'None': None}

        f_key_numbers = range(1, 13) # F1, ..., F12
        for i in f_key_numbers:
            modifiers_and_keys[f'F{i}'] = f'key.f{i}'

        use_key_operation = node_detail_form.get_chosen_value_by_name("use_key_operation", variable_handler)
        first_key = node_detail_form.get_chosen_value_by_name("first_key", variable_handler)
        second_key = node_detail_form.get_chosen_value_by_name("second_key", variable_handler)

        key1 = modifiers_and_keys[first_key]
        key2 = modifiers_and_keys[second_key]
        key3 = node_detail_form.get_chosen_value_by_name("third_key", variable_handler)

        code_base = """
        keyboard = pynput.keyboard.Controller()
        key = pynput.keyboard.Key

        """

        code_single_key = f"""
        key.press({key1})
        key.release({key1})
        """

        code_two_key = lambda key1, key2: f"""
        with keyboard.pressed({key1}):
            keyboard.press({key2})
            keyboard.release({key2})
        """

        code_three_key = lambda key1, key2, key3: f"""
        with keyboard.pressed({key1}):
            with keyboard.pressed({key2}):
                keyboard.press({key3})
                keyboard.release({key3})
        """

        if use_key_operation == 'Single key':
            code = code_base + code_single_key
        elif use_key_operation == 'Keyboard shortcut':
            if key3 == "" or key3.isspace():
                code = code_base + code_two_key(key1, key2)
            elif key2 is None:
                code = code_base + code_two_key(key1, key3)
            else:
                code = code_base + code_three_key(key1, key2, key3)

        return code

    def export_imports(self, *args):
        imports = ['import pynput.keyboard']
        return (imports)

    def make_flpl_node_dict(self, line_dict: dict) -> dict:
        node = {"type": "UseKey",
                "params": {"code_label": {"variable": None, "value": None}}}  # TODO: finish the node var
        return node


rpa_handlers_dict = {
    "Click": ClickHandler(),
    "ClickImage": ClickImageHandler(),
    "Write": WriteHandler(),
    'UseKey': UseKeyHandler()
}
