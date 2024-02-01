yaml_str = """\
--- !MyData
name: theName
param: parameter
data_file: file.csv
"""
import yaml
class Parent:
    def __init__(self, name, data_file, path=None):
        self._name = name
        self._data_file = (path + data_file) if path is not None else data_file

class Load(Parent):
    def __init__(self, name="",
             param = "param",
             useDataFile=False,
             data_file = "none",
             path=None,
             **kwargs):

        super().__init__(name=name,
                         data_file = data_file, path=path)

class LoadConstructor:
    def __init__(self, path=None):
        self._path = None

    def set_path(self, path):
        self._path = path

    def __call__(self, loader, node):
        values = loader.construct_mapping(node, deep=True)
        values['path'] = self._path
        return Load(**values)

load_constructor = LoadConstructor()
yaml.add_constructor(u'!MyData', load_constructor)

# using the above:
# set the default path
load_constructor.set_path('/my/path/to/csv/')
# parse YAML documents
for item in yaml.load_all(yaml_str):
    if type(item) is Load:
        print('data file:', item._data_file)


