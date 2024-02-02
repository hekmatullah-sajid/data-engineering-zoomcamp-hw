import re

def camel_to_snake(column_name):
    """
    Convert a CamelCase string to snake_case.
    """
    # Use regular expression to split words by capital letters
    words = re.findall(r'[A-Za-z][a-z0-9]*', column_name)
    # Join words with underscores and convert to lowercase
    snake_case_name = '_'.join(words).lower()
    return snake_case_name

print(camel_to_snake('VendorID'))