from collections import namedtuple

#from src.gui.gui_settings import DARK_MODE

DARK_MODE=False #removed dependency

class NodeTypeCategoriesManager:
    def __init__(self):
        categories=namedtuple("Categories",["webscraping","rpa","file_management","cleaning","database","api","data","control_flow","variable","model","mapping","plot","integration","unknown","custom"])
        self.categories=categories(webscraping="Webscraping",rpa="RPA",file_management="File Management",cleaning="Cleaning",database="Database",
                        api="API",data="Data",control_flow="Control Flow",variable="Variable",model="Model",
                        mapping="Mapping",plot="Plot",integration="Integration",unknown="Unknown", custom="Custom")
        
        self.category_color_dict={self.categories.webscraping:(230,160,0),
                             self.categories.rpa:(0,40,160),
                             self.categories.file_management:(0,80,120),
                             self.categories.cleaning:(0,100,100),
                             self.categories.database:(0,120,80),
                             self.categories.api:(0,160,40),
                             self.categories.data:(0,200,0),
                             self.categories.control_flow:(0,0,200),
                             self.categories.variable:(80,120,0),
                             self.categories.model:(120,80,0),
                             self.categories.mapping:(160,40,0),
                             self.categories.plot:(200,0,0),
                             self.categories.integration:(40,80,80),
                             self.categories.unknown:(40,40,120),
                             self.categories.custom: (0,100,20)
                             }
        
        if DARK_MODE:
            
            new_dict={}
            for k,v in self.category_color_dict.items():
                new_dict[k]=(255-v[0],255-v[1],255-v[2])
            print(new_dict)
            self.category_color_dict=new_dict


        
        self.icon_type_to_category_dict={}
        
ntcm=NodeTypeCategoriesManager()