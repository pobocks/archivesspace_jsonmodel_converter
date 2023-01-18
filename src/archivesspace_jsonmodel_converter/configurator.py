import attrs, yaml

from boltons.dictutils import OMD
from os.path import exists, expanduser
from os import environ as env



def ConfigSources(yaml_path):
    '''Helper method returning an :py:class:`boltons.dictutils.OrderedMultiDict` representing configuration sources (defaults, yaml)'''
    omd = OMD()
    yaml_path = expanduser(yaml_path)

    # Populate asnake config with defaults for local devserver
    omd.update({
        'asnake_config': {
            'baseurl'         : 'http://localhost:4567',
            'username'        : 'admin',
            'password'        : 'admin',
            'retry_with_auth' : True
        }
    })

    if exists(yaml_path):
        with open(yaml_path, 'r') as f:
            omd.update_extend(yaml.safe_load(f))
    return omd

@attrs.define(slots=True, repr=False)
class AJCConfig:
    '''Configuration object.  Essentially a convenience wrapper over an instance of :class:`boltons.dictutils.OrderedMultiDict`'''
    config = attrs.field(converter=ConfigSources, default=attrs.Factory(lambda: env.get('AJC_CONFIG_FILE', "~/.archivesspace_jsonmodel_converter.yml")))

    def __setitem__(self, k, v):
        return self.config.add(k, v)

    def __getitem__(self, k):
        return self.config[k]

    def __contains__(self, k):
        return k in self.config

    def update(self, *args, **kwargs):
        '''adds a set of configuration values in 'most preferred' position (i.e. last updated wins). See :meth:`boltons.dictutils.OrderedMultiDict.update_extend`
in the OMD docs'''
        return self.config.update_extend(*args, **kwargs)

    def __repr__(self):
        return "AJCConfig({})".format(self.config.todict())
