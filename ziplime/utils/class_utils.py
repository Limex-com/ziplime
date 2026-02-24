import importlib


def load_class(module_name: str, class_name: str):
    # Import the module
    module = importlib.import_module(module_name)
    # Get the class from the module
    cls = getattr(module, class_name)
    return cls
