import attrs, yaml

from boltons.dictutils import OMD
from os.path import exists, expanduser
from os import environ as env
from asnake.aspace import ASnakeClient
from .crosswalker import Crosswalk
import psycopg

from .logger import get_logger


log = get_logger('configurator')
def ConfigSources(yaml_path):
    '''Helper method returning an :py:class:`boltons.dictutils.OrderedMultiDict` representing configuration sources (defaults, yaml)'''
    omd = OMD()
    yaml_path = expanduser(yaml_path)

    # Populate asnake, postgres, and sqlite config with defaults for local devserver
    omd.update({
        'logging_config': {
            # filename: 'file_to_log_to.log',
            'level': 'INFO',
            'stream_json': True
        },
        'asnake_config': {
            'baseurl'         : 'http://localhost:4567',
            'username'        : 'admin',
            'password'        : 'admin',
            'retry_with_auth' : True
        },
        'postgres_config': {
            'host': 'localhost'
        },
        'crosswalk_config': {
            'name': 'crosswalk',
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

    def dynamic_configuration(self):
        if not 'd' in self.config:
            self.config['d'] = d = {} # store in config, d is a local convenience alias

        # ArchivesSnake
        d['aspace'] = ASnakeClient(**self.config['asnake_config'])

        # PostgreSQL
        try:
            d['postgres'] = psycopg.connect(**self.config['postgres_config'])
        except Exception as error:
            log.error('Database error', error=error)
            print(f'PostgreSQL DB Error: {error}')
            if d['postgres']:
                d['postgres'].close()

        # Crosswalk (sqlite)
        d['crosswalk'] = Crosswalk(self.config['crosswalk_config']['name'])
    def __repr__(self):
        return "AJCConfig({})".format(self.config.todict())
