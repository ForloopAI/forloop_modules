from forloop_modules.function_handlers.auxilliary.abstract_function_handler import AbstractFunctionHandler
from forloop_modules.function_handlers.auxilliary.form_dict_list import FormDictList
from forloop_modules.function_handlers.auxilliary.node_type_categories_manager import ntcm
from forloop_modules.function_handlers.auxilliary.docs import Docs
import forloop_modules.queries.node_context_requests_backend as ncrb
import dash
from dash import html
import threading
        

# HTML Elements
class DivHandler(AbstractFunctionHandler):
    icon_type = 'div'
    fn_name = 'Div'
    type_category = ntcm.categories.custom
    docs = Docs(description="HTML Div container.")
    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList(docs=self.docs)
        fdl.label("Div content:")
        fdl.entry(name="children", text="", category="arguments")
        return fdl
    def direct_execute(self, children):
        return f"html.Div({children})"

class SpanHandler(AbstractFunctionHandler):
    icon_type = 'span'
    fn_name = 'Span'
    type_category = ntcm.categories.custom
    docs = Docs(description="HTML Span inline element.")
    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList(docs=self.docs)
        fdl.label("Span text:")
        fdl.entry(name="text", text="", category="arguments")
        return fdl
    def direct_execute(self, text):
        return f"html.Span('{text}')"

class AnchorHandler(AbstractFunctionHandler):
    icon_type = 'a'
    fn_name = 'Anchor'
    type_category = ntcm.categories.custom
    docs = Docs(description="HTML Anchor (link) element.")
    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList(docs=self.docs)
        fdl.label("Link text:")
        fdl.entry(name="text", text="", category="arguments")
        fdl.label("Href:")
        fdl.entry(name="href", text="", category="arguments")
        return fdl
    def direct_execute(self, text, href):
        return f"html.A('{text}', href='{href}')"

# Dash Core Components
class DashIntervalHandler(AbstractFunctionHandler):
    icon_type = 'interval'
    fn_name = 'DashInterval'
    type_category = ntcm.categories.custom
    docs = Docs(description="Dash dcc.Interval component.")
    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList(docs=self.docs)
        fdl.label("Interval ID:")
        fdl.entry(name="id", text="interval", category="arguments")
        fdl.label("Interval (ms):")
        fdl.entry(name="interval", text="1000", category="arguments")
        return fdl
    def direct_execute(self, id, interval):
        return f"dcc.Interval(id='{id}', interval={interval})"

class DashInputHandler(AbstractFunctionHandler):
    icon_type = 'input'
    fn_name = 'DashInput'
    type_category = ntcm.categories.custom
    docs = Docs(description="Dash dcc.Input component.")
    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList(docs=self.docs)
        fdl.label("Input ID:")
        fdl.entry(name="id", text="input", category="arguments")
        fdl.label("Placeholder:")
        fdl.entry(name="placeholder", text="", category="arguments")
        return fdl
    def direct_execute(self, id, placeholder):
        return f"dcc.Input(id='{id}', placeholder='{placeholder}')"

# Dash Components (including Bootstrap)
class DashButtonHandler(AbstractFunctionHandler):
    icon_type = 'button'
    fn_name = 'DashButton'
    type_category = ntcm.categories.custom
    docs = Docs(description="Dash Button component (can be Bootstrap or core).")
    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList(docs=self.docs)
        fdl.label("Button text:")
        fdl.entry(name="text", text="Button", category="arguments")
        return fdl
    def direct_execute(self, text):
        return f"dbc.Button('{text}')"

class DashRowHandler(AbstractFunctionHandler):
    icon_type = 'row'
    fn_name = 'DashRow'
    type_category = ntcm.categories.custom
    docs = Docs(description="Dash Row component (can be Bootstrap or core).")
    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList(docs=self.docs)
        fdl.label("Row children:")
        fdl.entry(name="children", text="", category="arguments")
        return fdl
    def direct_execute(self, children):
        return f"dbc.Row({children})"

class DashColHandler(AbstractFunctionHandler):
    icon_type = 'col'
    fn_name = 'DashCol'
    type_category = ntcm.categories.custom
    docs = Docs(description="Dash Col component (can be Bootstrap or core).")
    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList(docs=self.docs)
        fdl.label("Col children:")
        fdl.entry(name="children", text="", category="arguments")
        return fdl
    def direct_execute(self, children):
        return f"dbc.Col({children})"

class DashServerHandler(AbstractFunctionHandler):
    icon_type = 'dash_server'
    fn_name = 'DashServer'
    type_category = ntcm.categories.custom
    docs = Docs(description="Start a Dash server with customizable port and debug mode. Stores the Dash app as a variable in ncrb.")
    def make_form_dict_list(self, *args, node_detail_form=None):
        fdl = FormDictList(docs=self.docs)
        fdl.label("Port:")
        fdl.entry(name="port", text="8050", category="arguments")
        fdl.label("Debug mode:")
        fdl.entry(name="debug", text="True", category="arguments")
        fdl.label("App variable name:")
        fdl.entry(name="app_var", text="dash_app", category="arguments")
        fdl.button(function=self.execute, function_args=node_detail_form, text="Execute", focused=True)
        return fdl
    def direct_execute(self, port, debug, app_var, node_detail_form=None):
        print("Starting Dash server...")
        import threading
        app = dash.Dash(__name__)
        app.layout = html.Div(['Hello, Dash!'])
        # Store the app as a variable in ncrb
        params_dict = {app_var: app}
        #if node_detail_form is not None:
        #    ncrb.update_node_by_uid(node_detail_form.node_uid, params=params_dict)
        def run_server():
            app.run_server(port=int(port), debug=(str(debug).lower() == 'true'))
        threading.Thread(target=run_server, daemon=True).start()

    def execute(self, node_detail_form):
        print("Executing Dash server...")
        port = node_detail_form.get_chosen_value_by_name("port", None)
        debug = node_detail_form.get_chosen_value_by_name("debug", None)
        app_var = node_detail_form.get_chosen_value_by_name("app_var", None)
        self.direct_execute(port, debug, app_var, node_detail_form)


dash_handlers_dict = {
    "Div": DivHandler(),
    "Span": SpanHandler(),
    "Anchor": AnchorHandler(),
    "DashInterval": DashIntervalHandler(),
    "DashInput": DashInputHandler(),
    "DashButton": DashButtonHandler(),
    "DashRow": DashRowHandler(),
    "DashCol": DashColHandler(),
    "DashServer": DashServerHandler(),
}