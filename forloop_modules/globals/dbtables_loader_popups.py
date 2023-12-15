from typing import Optional

from forloop_modules.globals.database_utilities_handler import duh

import forloop_modules.queries.node_context_requests_backend as ncrb

import forloop_modules.queries.context_request_backend_auxiliary_functions as crbaf
from forloop_modules.globals.active_entity_tracker import aet


def infer_database_structure(db_connection):
    # table_dict,foreign_keys=get_table_dict_and_fk(db_details=db_details)

    if db_connection is not None:
        table_dict = db_connection.table_dict
    # foreign_keys = db_connection.foreign_keys

    # table_dict_sample=dict(random.sample(table_dict.items(), 15))
    table_dict_sample = table_dict
    data = None
    for uid, db_connection_in_dict in duh.database_uid_db_connection_dict.items():
        if db_connection == db_connection_in_dict:
            data = [(uid, db_connection)]
            break
    get_tables_from_glc(data)


    # ge.glc.generate_db_tables_rects(db_connection, table_dict_sample.values())

    """
    if db_connection.db_details["DIALECT"]=="SQL Server":
        for k,v in table_dict_sample.items():
            parent_foreign_keys=v.get_foreign_keys_for_table(table_dict,foreign_keys)
            print("FK")
            #print(parent_foreign_keys)

            parent_index=[x.name for x in self.dbtables].index(v.name)
            for j,fk in enumerate(parent_foreign_keys):
                fk_column=v.columns[fk['parent_column_id']]

                self.dbtables[parent_index].dbkeys_dict[fk_column]="FK"
                print(self.dbtables[parent_index].dbkeys_dict)


                if fk['referenced_table'] in table_dict_sample.keys():
                    print("GENERATE")
                    #GENERATE ARROW FK

                    relative_from_rect_pos=[199,44+fk['parent_column_id']*18]
                    relative_to_rect_pos=[199,44+fk['referenced_column_id']*18]


                    referenced_index=[x.name for x in self.dbtables].index(fk['referenced_table'])
                    point1=[self.dbtables[parent_index].pos[0]+relative_from_rect_pos[0],self.dbtables[parent_index].pos[1]+relative_from_rect_pos[1]]
                    point2=[self.dbtables[referenced_index].pos[0]+relative_to_rect_pos[0],self.dbtables[referenced_index].pos[1]+relative_to_rect_pos[1]]
                    fk_arrow=Arrow([point1,point2],from_rect=self.dbtables[parent_index],to_rect=self.dbtables[referenced_index],relative_from_rect_pos=relative_from_rect_pos,relative_to_rect_pos=relative_to_rect_pos)

                    ge.gom.new(fk_arrow) #NOT SHOW ARROWS


                    #END GENERATION
            #self.dbtables[parent_index].initialize_dbkeys()
        """


# TODO: Find suitable name replacement (similar to get_tables)
# get_tables function moved from glc, no other reason for this name replacement
def get_tables_from_glc(*args, selected_tables: Optional[list[str]] = None):
    # TODO: Fix passing argument through args, or simplify it
    database_uid = args[0][0][0]
    db_connection = duh.database_uid_db_connection_dict[database_uid]
    is_successfull_connection = db_connection.test_database_connection()
    if not is_successfull_connection:
        ncrb.new_popup([500, 400], "RaiseNotConnectedPopup")
        return
    table_dict = db_connection.table_dict

    if selected_tables is None:
        selected_tables = list(table_dict.keys())

    col = 0
    height = 70

    for table_name, table in table_dict.items():
        if table_name in selected_tables:
            build_dbtable(table_name, table, db_connection, height)

            height += 35
            if height > 700:
                height = 70
                col += 1


def build_dbtable(table_name, table, db_connection, height):
    #create correct column format for api
    columns_api_format = get_columns_in_api_format(table.columns, table.types)

    #find corresponding database uid
    database_uid = None
    for key, value in duh.database_uid_db_connection_dict.items():
        if value is db_connection:
            database_uid = key
            break

    # pos = [gs.INITIAL_DB_TABLE_HORIZONTAL_OFFSET + col * 210, 100 + height]
    pos = [300, 300]
    ncrb.new_dbtable(table_name, pos, columns_api_format, is_rolled=False, database_uid=database_uid,
                    project_uid=aet.project_uid)


def get_columns_in_api_format(columns, types):
    """Transforms an array of columns and array of types into array of dicts with column name pair and column type pair

    column = ['id', 'name']
    types = ['int', 'str']
    ->
    columns_api_format = [{'name': 'id', 'type': 'int'}, {'name': 'name', 'type': 'str'}]    

    Args:
        columns (list): list of column names
        types (list): list of column types

    Returns:
        list[dict]: list of dicts that contains column name and column type
    """
    columns_api_format = []
    for key, typ in zip(list(columns), list(types)):
        entry = {
            'name': key,
            'type': typ,
            # TODO: solve db_key key
            'db_key': ""
        }
        columns_api_format.append(entry)
    return columns_api_format


def get_db_uid(node_detail_form):
    # TODO: WHY IS IT NEEDED TO REFRESH NODE DETAIL FORM TO geT DB UID??
    db_uid = None
    nodes = crbaf.get_and_subset_requested_objects(ncrb.get_all_nodes, applied_key_sequence=["nodes"], filter_by_project_uid=True)
    for node in nodes:
        if node['uid'] == node_detail_form.node_uid:
            db_uid = node["fields"][0]["value"]
            break

    return db_uid

# TODO: Find suitable name replacement (similar to get_tables_from_glc)
def get_tables(node_detail_form):
    """
    Find db connection and generate temporal tables rectangles
    :param node_detail_form: node_detail_form with db uid in fields
    """
    db_uid = get_db_uid(node_detail_form)

    selected_db_connection = duh.database_uid_db_connection_dict[db_uid]
    valid_db_connection = selected_db_connection.test_database_connection()
    if valid_db_connection:
        infer_database_structure(db_connection=selected_db_connection)
    else:
        ncrb.new_popup([500, 400], "RaiseNotConnectedPopup")

def get_specific_table(node_detail_form):
    """
    Create popup for choice of (up to) several specific tables to be added to platform from db
    :param rect:
    :type rect:
    :return:
    :rtype:
    """
    db_uid = get_db_uid(node_detail_form)
    
    selected_db_connection = duh.database_uid_db_connection_dict[db_uid]
    connection_ok = selected_db_connection.test_database_connection()
    if connection_ok:
        duh.last_active_database = selected_db_connection
        ncrb.new_popup([500, 400], "GetSpecificTablesPopup")
        # Commented code removes FE dependency, also this method is called every second, so we can afford commenting this out
        # ncr.corrective_popup_popupform_reflection()
    else:
        ncrb.new_popup([500, 400], "RaiseNotConnectedPopup")

def manage_tables(node_detail_form):
    db_uid = None
    nodes = crbaf.get_and_subset_requested_objects(ncrb.get_all_nodes, applied_key_sequence=["nodes"], filter_by_project_uid=True)
    for node in nodes:
        if node['uid'] == node_detail_form.node_uid:
            db_uid = node["fields"][0]["value"]
            break
    selected_db_connection = duh.database_uid_db_connection_dict[db_uid]
    connection_ok = selected_db_connection.test_database_connection()
    if connection_ok:
        duh.last_active_database = selected_db_connection
        ncrb.new_popup([500, 400], "ManageDBTablesPopup")
        # Commented code removes FE dependency, also this method is called every second, so we can afford commenting this out
        # ncr.corrective_popup_popupform_reflection()
    else:
        ncrb.new_popup([500, 400], "RaiseNotConnectedPopup")
