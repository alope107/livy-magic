from livyclient import LivyClient
from IPython.core.magic import Magics, magics_class, cell_magic, line_magic, needs_local_scope
from IPython.core.magic_arguments import (argument, magic_arguments,
    parse_argstring)

@magics_class
class LivyMagic(Magics):
    def __init__(self, shell):
        Magics.__init__(self, shell=shell)
        self.clients = {}


    @magic_arguments()
    @argument('-h', '--host', help='Host of Livy Server', default="http://localhost:8998")
    @argument('-k', '--kind', help='Kind of language to use. Options are spark, pyspark, or sparkR', default="spark")
    @cell_magic('spark')
    def execute(self, line, cell = '', local_ns={}):
        command = line
        args = parse_argstring(self.execute, command)

        host = args.host
        kind = args.kind

        identifier = (host, kind)
        
        if identifier not in self.clients.keys():
            self.clients[identifier] = LivyClient(host=host, kind=kind)        

        return self.clients[identifier].execute(cell)

def load_ipython_extension(ip):
    ip.register_magics(LivyMagic)
