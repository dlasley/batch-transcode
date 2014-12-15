#/usr/bin/env python
'''
MkvMerge wrappers
'''
import sh
import os
from transcode.console import Console
from transcode.exceptions import FileFormatError
from transcode.exceptions import OutputParseError

class MkvMerge(Console):
    
    __path__ = 'mkvmerge'
    
    def __init__(self, ):
        super(MkvMerge, self).__init__()
    
    def get_info(self, path):
        '''
        Return a MediaFile object corresponding to the file at path
            (mkvmerge --identify-verbose)
        @param  str path    Media file path
        @return VideoFile
        '''
        
        self.__valid_file(path)
        
        info_regex = re.compile('Track ID ([0-9]{1,2}): (\w+) \((.+)\) \[(([\w/]+:.+ ?)*)\]')
        info = self.command_with_priority( [ self.__path__, '--identify-verbose', file_path ] )[1]

        info_out = {}
        for match in info_regex.finditer(info):
            #   1-Track Id, 2-Track Type, 3-Codec ID, 4-Verbose Attrs
            try:
                raise OutputParseError(
                    'Double ID assigning in MkvMerge.get_info(): %s' % info_out[match.group(1)],
                    info
                )
            except KeyError:
                track_info = {
                    'ID'        :   match.group(1),
                    'track_type':   match.group(2),
                }
                for attr_pairs in match.group(4).split(' '):
                    attr_pairs = attr_pairs.split(':')
                    if not len(attr_pairs) == 2: raise Exception('Attr/Pair cnt problem %s. %s' % (len(attr_pairs), attr_pairs))
                    try:
                        raise Exception('Double assigning %s. Already "%s", New "%s"' % (attr_pairs[0],track_info[attr_pairs[0]],attr_pairs[1]))
                    except KeyError:
                        track_info[attr_pairs[0]] = attr_pairs[1]
                info_out[match.group(1)] = track_info
        return info_out
    
    def __valid_file(self, path):
        '''
        Validate MKV files, raise FileFormatError if bad
        @param  str path    File path
        @raises FileFormatError if file is not MKV
        '''
        if not path.lower().endswith('.mkv'):
            raise FileFormatError(
                'Tried to MkvMerge.get_info on non-mkv file', path
            )