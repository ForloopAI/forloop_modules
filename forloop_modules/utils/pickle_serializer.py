"""Helper file which solves all serialization/conversion between objects, values of variables and pickle files"""

from pathlib import Path
from contextlib import suppress
import shutil

import os
import pickle
import forloop_modules.flog as flog



def save_data_dict_to_pickle_folder(data_dict, folder, clean_existing_folder=True):
    """data_dict ... key=variable_name, value=data
    e.g. key=df1, value=object of type pd.DataFrame()
    """
    if clean_existing_folder:
        with suppress(FileNotFoundError):
            shutil.rmtree(folder, ignore_errors=True) #recursive variant of os.rmdir(pickle_folder)

    os.makedirs(folder, exist_ok=True) #create folder if doesnt exist

    for k, v in data_dict.items():
        print("datadict",k,v)
        with Path(folder, k+".pickle").open(mode='wb') as pickle_file:
            pickle.dump(v, pickle_file) #to serialize DF

def load_data_dict_from_pickle_folder(folder):
    
    data_dict={}
    
    for root, dirs, files in os.walk(folder):
        for file in files:
            try:
                with open(folder+"\\"+file, 'rb') as pickle_file:
                    object_name=file.split(".pickle")[0]
                    data_dict[object_name]=pickle.load(pickle_file)
            except FileNotFoundError:
                flog.warning("Pickle file was not processed",file)

    return(data_dict)


def save_pickle_data(filename, json_dict):
    pipeline_folder = "/".join(filename.split("/")[0:-1])
    pipeline_name = filename.split("/")[-1].split(".flpl")[0]
    # pickle_folder = pipeline_folder+"//"+pipeline_name # This sometimes throws an error on Mac
    pickle_folder = Path(pipeline_folder, pipeline_name)

    data_dict=json_dict["pickle_data"]
    save_data_dict_to_pickle_folder(data_dict, pickle_folder)
    # with suppress(FileNotFoundError):
    #     shutil.rmtree(pickle_folder, ignore_errors=True) #recursive variant of os.rmdir(pickle_folder)

    # os.makedirs(pickle_folder, exist_ok=True) #create folder if doesnt exist

    # for pipeline_filename, pipeline_data in json_dict["pickle_data"].items():
    #     with Path(pickle_folder, pipeline_data, ".pickle").open(mode='wb') as pickle_file:
    #         pickle.dump(pipeline_data, pickle_file) #to serialize DF

    json_dict.pop("pickle_data")

    return json_dict



def read_pickle_data(filename,json_dict):
    """enriches json_dict for pickle data"""
    pipeline_folder="/".join(filename.split("/")[0:-1])
    pipeline_name=filename.split("/")[-1].split(".flpl")[0]
    pickle_folder=pipeline_folder+"//"+pipeline_name

    data_dict=load_data_dict_from_pickle_folder(pickle_folder)

    json_dict["pickle_data"]=data_dict

    
    
    # for root, dirs, files in os.walk(pickle_folder):
    #     for file in files:
    #         try:
    #             with open(pickle_folder+"\\"+file, 'rb') as pickle_file:
    #                 object_name=file.split(".pickle")[0]
    #                 json_dict["pickle_data"][object_name]=pickle.load(pickle_file)
    #         except FileNotFoundError:
    #             flog.warning("Pickle file was not processed",file)

    return(json_dict)


