#/usr/bin/env python

'''
Media file and stream objects. 
'''


class BaseInfo(object):
    '''
    Base Info class, all others derive from it. Mainly just helpers
    '''
    def _set_default_attrs(self, attrs, **kwargs):
        for i in attrs:
            try:
                setattr(self, i, kwargs[i])
            except KeyError:
                setattr(self, i, None)


##  File Level Descriptors
class MediaFile(BaseInfo):
    '''
    File info, base class attrs:
    '''
    def __init__(self, ):
        pass
    

class VideoFile(MediaFile):
    '''
    File info specific to videos. Includes all MediaFile attrs, plus:
    '''
    def __init__(self, ):
        pass
    
    
class AudioFile(MediaFile):
    '''
    File info specific to audio. Includes all MediaFile attrs, plus:
    '''
    def __init__(self, ):
        pass


##  Internal Stream Descriptors
class MediaStream(BaseInfo):
    '''
    Info regarding one stream in a file. Base class attrs:
    '''
    def __init__(self, **kwargs):
        attrs = [ 'format', 'codec_id', 'bit_rate', 'duration',
                  'bytes',  ]
        self._set_default_attrs(attrs, kwargs)
    

class VideoStream(MediaStream):
    '''
    Info specific to a video stream in a file. Includes all MediaStream attrs, plus:
    '''
    def __init__(self, **kwargs):
        attrs = [ 'format_info', ]
        super(VideoStream, self).__init__(kwargs)
        self._set_default_attrs(attrs, kwargs)
        
    

class AudioStream(MediaStream):
    '''
    Info specific to an audio stream in a file. Includes all MediaStream attrs, plus:
    '''
    def __init__(self, **kwargs):
        attrs = [ 'format_info', ]
        super(AudioStream, self).__init__(kwargs)
        self._set_default_attrs(attrs, kwargs)
    
    
class SubStream(MediaStream):
    '''
    Info specific to a subtitle stream in a file. Includes all MediaStream attrs, plus:
    '''
    def __init__(self, **kwargs):
        attrs = [ 'codec_info', ]
        super(SubStream, self).__init__(kwargs)
        self._set_default_attrs(attrs, kwargs)

