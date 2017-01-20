import errno
import logging
import os
import subprocess
import uuid

import ansible.executor.module_common as module_common
from ansible import modules

LOG = logging.getLogger(__name__)

try:
    import json
except ImportError:
    import simplejson as json


class AnsibleExecutableGenerationFailed(Exception):
    def __init__(self, module_path=None, arguments=None, err=None):
        self.message = "Executabe could not be generated for module" \
                       " %s , with arguments %s. Error: %s" % (
                           str(module_path), str(arguments), str(err))


class AnsibleRunner(object):
    """Class that can be used to run ansible modules

    """

    def __init__(self, module_path, exec_path, **kwargs):
        exec_path = os.path.expandvars(exec_path)
        self.executable_module_path = exec_path + str(uuid.uuid4())
        self.module_path = modules.__path__[0] + "/" + module_path
        if not os.path.isfile(self.module_path):
            LOG.error("Module path: %s does not exist" % self.module_path)
            raise ValueError
        if kwargs == {}:
            LOG.error("Empty argument dictionary")
            raise ValueError
        else:
            self.argument_dict = kwargs

    def __generate_executable_module(self):
        modname = os.path.basename(self.module_path)
        modname = os.path.splitext(modname)[0]
        try:
            (module_data, module_style, shebang) = \
                module_common.modify_module(
                    modname,
                    self.module_path,
                    self.argument_dict,
                    task_vars={}
                )
        except Exception as e:
            LOG.error("Could not generate executable data for module"
                      ": %s. Error: %s" % (self.module_path, str(e)))
            raise AnsibleExecutableGenerationFailed(
                self.module_path,
                self.executable_module_path,
                str(e)
            )
        if not os.path.exists(os.path.dirname(self.executable_module_path)):
            try:
                os.makedirs(os.path.dirname(self.executable_module_path))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
        with open(self.executable_module_path, 'w') as f:
            f.write(module_data)
        os.system("chmod +x %s" % self.executable_module_path)

    def __destroy_executable_module(self):
        os.remove(self.executable_module_path)

    def run(self):
        self.__generate_executable_module()

        cmd = subprocess.Popen(
            self.executable_module_path,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        out, err = cmd.communicate()
        try:
            result = json.loads(out)
        except ValueError:
            result = out

        self.__destroy_executable_module()

        return result, err
