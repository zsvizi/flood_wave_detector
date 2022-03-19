class JsonHelper:
    @staticmethod
    def write(filepath: str, obj: Union[dict, tuple]):
        with open(filepath, 'w') as file:
            json.dump(obj=obj, fp=file, indent = 4)
    @staticmethod
    def read(filepath: str):
        with open(filepath, 'r') as file:
            return json.load(file)
