
def parse_comboentry_input(input_value: list[str]):
    input_value = input_value[0] if isinstance(input_value, list) and len(input_value) > 0 else input_value
    
    return input_value

