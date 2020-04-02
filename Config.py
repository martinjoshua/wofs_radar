from configparser import ConfigParser
import warnings

section_names = 'DEFAULT', 'MRMS', 'OPAWS', 'RASS'

class AppConfiguration(object):  

    def __get_value__(self, val):
        if val.lower().strip() == 'true':
            return True
        elif val.lower().strip() == 'false':
            return False
        else:
            return val

    def __init__(self, *file_names):
        parser = ConfigParser()
        parser.optionxform = str  
        found = parser.read(file_names)
        if not found:
            raise ValueError('No config file found!')
        for name in section_names:
            for pair in parser.items(name):
                attr = (name+'_'+pair[0]).lower()
                v = self.__get_value__(pair[1])
                setattr(self, attr, v)
                self.__dict__.update({ attr: v })

    def __getattr__(self, name):
        ''' will only get called for undefined attributes '''
        warnings.warn('No member "%s" contained in settings config.' % name)
        return ''

settings = AppConfiguration('app.cfg', 'app.test.cfg')
