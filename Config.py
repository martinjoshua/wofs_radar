from configparser import ConfigParser
import warnings

section_names = 'DEFAULT', 'MRMS', 'JOBS', 'OPAWS'

class AppConfiguration(object):  
    def __init__(self, *file_names):
        parser = ConfigParser()
        parser.optionxform = str  
        found = parser.read(file_names)
        #self.opaws_feed = ''
        if not found:
            raise ValueError('No config file found!')
        for name in section_names:
            iter(lambda x: setattr(self, (name+'_'+x[0]).lower(), ''), parser.items(name))
            self.__dict__.update(map(lambda x: ((name+'_'+x[0]).lower(), x[1]), parser.items(name)))
    def __getattr__(self, name):
        ''' will only get called for undefined attributes '''
        warnings.warn('No member "%s" contained in settings config.' % name)
        return ''

settings = AppConfiguration('app.cfg', 'app.test.cfg')
