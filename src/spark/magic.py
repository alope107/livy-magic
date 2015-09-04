from livyclient import LivyClient
from IPython.core.magic import Magics, magics_class, cell_magic, line_magic, needs_local_scope
from IPython.core.magic_arguments import (argument, magic_arguments,
    parse_argstring)

@magics_class
class LivyMagic(Magics):
    """Runs SQL statement through Spark using provided SQLContext.

    Provides the %sparksql magic.
    """

    def __init__(self, shell):
        Magics.__init__(self, shell=shell)
        self.client = LivyClient()


    @cell_magic('spark')
    def execute(self, line, cell = '', local_ns={}):
        return self.client.execute(cell)["output"]["data"]["text/plain"]

def load_ipython_extension(ip):
    ip.register_magics(LivyMagic)
