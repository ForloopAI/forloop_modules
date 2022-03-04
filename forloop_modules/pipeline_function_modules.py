import abc
import random
import doclick.doclick_core as dc
import doclick.doclick_image as di

import os


class AbstractFunctionHandler(abc.ABC):
    """Forces all handlers to have defined abstract methods"""
    @abc.abstractmethod
    def make_form_dict_list(self,*args):
        pass
    
    @abc.abstractmethod
    def direct_execute(self,*args):
        pass
    
    def export_code(self,*args):
        code="""
        #Not implemented
        """
        return(code)

        
    def export_imports(self,*args):
        imports=[]
        return(imports)
    


class StartHandler(AbstractFunctionHandler):
    icon_type = "Start"
    fn_name = "Start"

    def make_form_dict_list(self, *args):
        form_dict_list = [{"Label": self.fn_name}]
        return form_dict_list


    def direct_execute(self, *args):
        # def __new__(cls, *args, **kwargs):
        """Do nothing"""
        pass

    def export_code(self,*args):
        code="""
        #start pipeline
        """
        return(code)
        
    def export_imports(self,*args):
        imports=[]
        return(imports)
    
    

class FinishHandler(AbstractFunctionHandler):
    icon_type = "Finish"
    fn_name = "Finish"

    def make_form_dict_list(self, *args):
        form_dict_list = [{"Label": self.fn_name}]
        return form_dict_list

    def direct_execute(self, *args):
        # def __new__(cls, *args, **kwargs):
        """Finish - TODO: Refactor to not be dependent on ge"""
        # psm.reset(args)
        
        
    def export_code(self,*args):
        code="""
        #finish pipeline
        """
        return(code)
        
    def export_imports(self,*args):
        imports=[]
        return(imports)




class WaitHandler(AbstractFunctionHandler):
    icon_type = "Wait"
    fn_name = "Wait"
    
    def make_form_dict_list(self, *args):
        form_dict_list = [{"Label": self.fn_name},
            {"Label": "Milliseconds:", "Entry": {"name": "milliseconds", "text": "1000"}},
            {"Label": "Add random ms:", "Entry": {"name": "rand_ms", "text": "0"}}]
        return form_dict_list

    def direct_execute(self, milliseconds, rand_ms, *args):
        # def __new__(cls, milliseconds, rand_ms, *args, **kwargs):
        milliseconds = int(milliseconds)
        rand_ms = int(rand_ms)
        milliseconds += random.randint(0, int(rand_ms))
        milliseconds = str(milliseconds)

        command = f'Wait({milliseconds})'
        dc.execute_order(command)
        
        
    def export_code(self,*args):
        code="""
        command = f'Wait({milliseconds})'
        dc.execute_order(command)
        """
        return(code)

        
    def export_imports(self,*args):
        imports=["import doclick.doclick_core as dc", "import random"]
        return(imports)




class ClickImageHandler: #TODO: AbstractFunctionHandler
    def __init__(self):
        self.icon_type = "ClickImage"
        self.fn_name = "Click Image"

    def make_form_dict_list(self, *args):

        form_dict_list = [
            {"Label": "Click Image"},
            {"Label": "Image path:", "Entry": {"name": "image_path", "text": ""}},
            {"Label": "Click offset X", "Entry": {"name": "offset_x", "text": ""}},
            {"Label": "Click offset Y", "Entry": {"name": "offset_y", "text": ""}}
        ]
        return form_dict_list

    def execute(self, args):
        image_path = args[0]
        click_offset_x = args[1]
        click_offset_y = args[2]
        img = di.take_screenshot()
        subimg = di.load_img(image_path)
        true_points = di.detect_subimg(img, subimg)
        coor = true_points[0]
        
        if args[1] == "":
            x = "0"
        else:
            x = str(int(click_offset_x) + coor[0])
        if args[2] == "":
            y = "0"
        else:

            y = str(int(click_offset_y) + coor[1])

        command = "Click(" + x + "," + y + ")"
        dc.execute_order(command)
        print("Clicked")
        
            
    def export_code(self,*args):
        code="""
        #Not implemented
        """
        return(code)

        
    def export_imports(self,*args):
        imports=["import doclick.doclick_core as dc","import doclick.doclick_image as di"]
        return(imports)


class WriteHandler: #TODO AbstractFunctionHandler
    def __init__(self):
        self.icon_type = "Write"
        self.fn_name = "Write"

    def make_form_dict_list(self, *args):
        form_dict_list = [
            {"Label": "Write"},
            {"Label": "Text:", "Entry": {"name": "text", "text": ""}}
        ]
        return form_dict_list

    def execute(self, args):
        dc.execute_order("Write(" + args[0] + ")")
        
        
    def export_code(self,*args):
        code="""
        dc.execute_order("Write(" + args[0] + ")")
        """
        return(code)

        
    def export_imports(self,*args):
        imports=["import doclick.doclick_core as dc"]
        return(imports)

########### PIPELINE HANDLERS #########################





class PythonScriptHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "PythonScript"
        self.fn_name = "Python Script"

    def make_form_dict_list(self, *args):
        form_dict_list = [
            {"Label": "Python Script"},
            {"Label": "Script filename", "Entry": {"name": "script", "text": "script.py"}},
            {"Label": "Directory path", "Entry": {"name": "dir_path", "text": ""}}
        ]
        return form_dict_list

    def direct_execute(self,script,dir_path=None,*args):
        """
        script - e.g. "test1.py"
        dir_path - e.g. "C:\\Users\\EUROCOM\\Documents\\Git\\ForloopAI\\forloop_platform_dominik\\" 
        """
        
        command="python"
        
        if dir_path!="" and dir_path is not None:
            os.chdir(dir_path)
        else:
            dir_path=""
        os.system("start cmd cd "+dir_path+script+" /k "+command+" "+dir_path+script)



class JupyterScriptHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "JupyterScript"
        self.fn_name = "Jupyter Script"

    def make_form_dict_list(self, *args):
        form_dict_list = [
            {"Label": "Jupyter Notebook Script"},
            {"Label": "Script filename", "Entry": {"name": "script", "text": "script.py"}}
        ]
        return form_dict_list

    def direct_execute(self,script,*args):
        
        command="runipy"
        location="C:\\Users\\EUROCOM\\Documents\\Git\\ForloopAI\\forloop_platform_dominik\\"
        
        os.chdir(location)
        os.system("start cmd cd "+location+script+" /k "+command+" "+location+script)






############ INTEGRATION HANDLERS ######################
class SlackNotificationHandler(AbstractFunctionHandler):
    def __init__(self):
        self.icon_type = "SlackNotification"
        self.fn_name = "Slack Notification"

    def make_form_dict_list(self, *args):
        form_dict_list = [
            {"Label": "Slack Notification"},
            {"Label": "Text:", "Entry": {"name": "text", "text": ""}}
        ]
        return form_dict_list

    def execute(self, args):
        print("EXECUTE")
        #dc.execute_order("Write(" + args[0] + ")")
     
        
     
    def direct_execute(self, text, *args):
        print("DIRECT EXECUTE")
        
    def export_code(self,*args):
        code="""
        """
        return(code)

        
    def export_imports(self,*args):
        #imports=["import doclick.doclick_core as dc"]
        imports=[]
        return(imports)


