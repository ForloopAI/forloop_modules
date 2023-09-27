import forloop_modules.flog as flog

# TODO: Implement control over executed code (some code should be forbidden to run)

def eval_expression(expression:str, globals_dict, locals_dict):
    """
    Evaluates the expression as follows: eval(expression, globals_dict, locals_dict)

    Parameters:
        expression:str ... expression/code to be evaluated
        globals_dict ... globals() or dict (if not globals() ==> globals() in the caller module shoud be updated by the returned globals_dict)
        locals_dict ... locals() or dict (if not locals() ==> locals() in the caller module shoud be updated by the returned locals_dict)

    Returns:
        obj: Any | None ... object received from the evaluated code
        globals_dict: dict ... globals_dict parameter updated by values from evaluation
        locals_dict: dict ... locals_dict parameter updated by values from evaluation
    """
    
    try:
        obj = eval(expression, globals_dict, locals_dict)
    except Exception as e:
        flog.error(f'Error during code evaluation:\n{e}')
    
    return obj

def exec_code(code:str, globals_dict, locals_dict):
    """
    Executes the code as follows: exec(expression, globals_dict, locals_dict)

    Parameters:
        code:str ... code to be evaluated
        globals_dict ... globals() or dict (if not globals() ==> globals() in the caller module shoud be updated by the returned globals_dict)
        locals_dict ... locals() or dict (if not locals() ==> locals() in the caller module shoud be updated by the returned locals_dict)

    Returns:
        globals_dict: dict ... globals_dict parameter updated by values from evaluation
        locals_dict: dict ... locals_dict parameter updated by values from evaluation
    """
    
    try:
        exec(code, globals_dict, locals_dict)
    except Exception as e:
        flog.error(f'Error during code execution:\n{e}')
