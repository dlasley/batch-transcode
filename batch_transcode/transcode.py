#!/usr/bin/env python
##  Transcode
#         
#    Video transcoding class
#    
#    @author     David Lasley, dave -at- dlasley -dot- net
#    @package    batch-transcode
#    @version    $Id$

import threading
import re
import xml.dom.minidom
import os
import subprocess
import time
import logging
import math
import hashlib

if os.name == 'nt':
    import win32api,win32process,win32con
    NICE_LVL = 0
    FFMPEG_LOCATION = os.path.join('Z:\\','Programs','Windows','ffmpeg-20130205-git-c2dd5a1-win64-static','bin','ffmpeg.exe')
    MKVTOOLNIX_DIR = os.path.join('Z:\\','Programs','Windows','mkvtoolnix')
    MKVMERGE_PATH = os.path.join(MKVTOOLNIX_DIR,'mkvmerge.exe')
    MKVEXTRACT_PATH = os.path.join(MKVTOOLNIX_DIR,'mkvextract.exe')
    MEDIAINFO_PATH = os.path.join('Z:\\','Programs','Windows','mediainfo','MediaInfo.exe')
    DEV_NULL = u'NUL'
    WINDOWS = True
else:
    #IO_NICE = 3 #< @todo #17
    NICE_LVL = 19
    FFMPEG_LOCATION = 'avconv'
    MKVMERGE_PATH = 'mkvmerge'
    MKVEXTRACT_PATH = 'mkvextract'
    MEDIAINFO_PATH = 'mediainfo'
    DEV_NULL = u'/dev/null'
    WINDOWS = False

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

class transcode(object):
    dry_runs        =   {
        'demux'     :   False,
        'transcode' :   False,
        'remux'     :   False,
        'dont_delete':  False,
    } 
    dir_permissions =   0777
    track_type_order=   ('Video','Audio','Text')
    settings_file = 'video_settings.xml'
    transcode_settings = {
        'x264'      :   {
                            
                        },
        'avconv'    :   {
                            'vcodec'    :   'libx264',
                            'preset'    :   'veryslow', #  Ultrafast, superfast, veryfast, faster, fast, medium, slow, slower, veryslow, placebo
                            'an'        :   True,   #   No audio
                            'y'         :   True,   #   Overwrite
                            'sn'        :   True,   #   No subs
                            'tune'      :   'film',
                            'level'     :   '41',
                        },
        'fix_dvds'  :   False,
        'deinterlace':  False,
        'container' :   'mkv',
        'br_percent':   False   #   Percent of bitrate (INT), False is go with program defaults
    }
    #   Base 10 on purpose!
    to_bit_map      =   {
        'Gbps'  :   1000000000,#1073741824,
        'Mbps'  :   1000000,#1048576,
        'Kbps'  :   1000,#1024,
        'bps'   :   1
    }
    file_extension_map = {
        'failsafes' :   {
            'Video' :   'avc',
            'Audio' :   'ac3',
            'Text'  :   'sup'
        },
        'regexes':{
            'MPEG'  :   'mpg',
            'DTS'   :   'dts',
            'PGS'   :   'sup',
            'VOBSUB':   'sub',
            'PCM'   :   'pcm',
        }
    }
    vid_exts        =   ['.mkv','.m4v','.mp4','.mpg','.avi']
    native_language =   ('English','eng')
    THREADS = 1
    def __init__(self,out_dir,debug=False):
        self.out_dir = out_dir
        #   Generate the dirs
        self.encode_dir      =   os.path.join(out_dir,'encodeBox')
        self.finished_dir    =   os.path.join(out_dir,'finished')
        self.error_dir       =   os.path.join(out_dir,'errored')
        self.log_file        =   os.path.join(self.error_dir,'error_log.log')
        for i in (self.encode_dir,self.finished_dir,self.error_dir):
            if not os.path.isdir(out_dir):
                os.mkdir(i, self.dir_permissions)

        self.__debug = debug
        self.cleanup_files = []

    def encode_directory(self,inpath):
        '''
            Encodes an entire directory
            
            @param  String  inpath  Directory to encode
            
            @return List    Newly created files
        '''
        self.new_files = []
        self.worker_threads = {}
        def thread(root,new_root,file_name,extension,transcode_settings):
            self.new_files.append( self.encode_it(os.path.join(root,'%s%s'%(file_name,extension)), os.path.join(new_root, '%s%s'%(file_name,extension)),transcode_settings) )
            del self.worker_threads[file_name]
            if True not in self.dry_runs:   #<  Delete the file if not testing or specified
                os.unlink(os.path.join(root,file_name,extension))
                logging.debug('DELETED: %s'%os.path.join(root,file_name,extension))
        for root, dirs, files in os.walk(inpath):
            dirs.sort()
            files.sort()
            if '.Apple' not in root:
                new_root = os.path.join(self.finished_dir,root.replace(inpath,''))
                if not os.path.isdir(new_root):
                    os.mkdir(new_root)
                try:
                    with open(os.path.join(root,self.settings_file)) as f: pass
                    transcode_settings = self.parse_video_settings(os.path.join(root,self.settings_file))
                    print transcode_settings
                except IOError:
                    transcode_settings = {}
                #exit()
                for file_name in files:
                    file_name,extension = os.path.splitext(file_name)
                    if extension in self.vid_exts:
                        #print len(worker_threads)
                        while len(self.worker_threads) >= self.THREADS:
                            time.sleep(10)  #<   Wait a bit
                            #print worker_thread
                        #else:
                        self.worker_threads[file_name] = (threading.Thread(target=thread,name=file_name,args=(root,new_root,file_name,extension,transcode_settings)))
                        self.worker_threads[file_name].daemon = True
                        self.worker_threads[file_name].start()
                        #print 'Removed: %s/%s%s'%(root,file_name,extension)
                        #print new_files
                        #exit()
        #print new_files
    def encode_it(self,file_path,new_file,transcode_settings={}):
        '''
            Encode wrapper, whole process
            
            @param  String  file_path   In file
            
            @return String  Out file
        '''
        try:
            media_info = self.media_info(file_path)
            cleanup_files = []
            lng_codes = transcode.lng_codes()
            demuxed = self.demux(file_path,media_info,self.encode_dir,dry_run=self.dry_runs['demux'])
            cleanup_files.extend(demuxed)
            duped = self.compare_tracks(demuxed)
            #media_info['tracks'][i+1]   :   mux_files[i]
            for vid_id in media_info['id_maps']['Video']:
                demuxed[vid_id-1] = self.transcode(
                    file_path,
                    os.path.join(
                        self.encode_dir,
                        u'%s.%s'%(os.path.basename(file_path),
                                  self.transcode_settings['container'])
                    ),
                    media_info['tracks'][vid_id],
                    dry_run=self.dry_runs['transcode']
                )
                cleanup_files.append(demuxed[vid_id-1])
                logging.debug('Transcoded %s' % demuxed[vid_id-1])
            track_order = self.choose_track_order(media_info)
            logging.debug('Track Order Chosen!')
            new_file = transcode.remux(demuxed,media_info,new_file,duped,track_order,dry_run=self.dry_runs['remux'])
            logging.debug('New File Success %s' % new_file)
            
        except Exception as e: #< Trigger error, leave file in place
            logging.error(repr(e))
            #   Implement #20
            fh = open(self.log_file, 'a')
            fh.write( repr(e) + "\n" ) 
            fh.close()
        
        #   Delete leftover files
        for cleanup_file in cleanup_files:
            try:
                logging.debug( 'Removing %s' % cleanup_file)
                os.remove(cleanup_file)
            except OSError:
                logging.error('Could not delete %s' % cleanup_file)
        return new_file
    
    '''     Static Methods      '''
    @staticmethod
    def lng_codes():
        ##  Returns Available Language Codes    @deprecated
        #   @return Dict    Language Codes, { short_code : long_code }
        mkvmerge_lng_codes = subprocess.check_output([MKVMERGE_PATH, '--list-languages'])
        lng_codes = {}
        for row in mkvmerge_lng_codes.split('\n'):
            cols = row.split('|')
            try:
                lng_codes[cols[1].strip()] = cols[0].strip()
            except IndexError:
                continue #< next iteration
        logging.debug(repr(lng_codes))
        return lng_codes
    
    @staticmethod
    def command_with_priority(command,shell=False,cwd='./'):
        ##  Runs a command using subprocess. Sets the priority.
        #   @param  List    command Command to execute
        #   @param  Bool    shell   Use shell
        #   @param  Str     cwd     Current working directory
        #   @return Tuple   (returncode, stdoutdata, stderrdata)
        if WINDOWS:
            process = subprocess.Popen(command,shell=shell,cwd=cwd,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    )
            """ http://code.activestate.com/recipes/496767/ """
            priorityclasses = [win32process.IDLE_PRIORITY_CLASS,
                               win32process.BELOW_NORMAL_PRIORITY_CLASS,
                               win32process.NORMAL_PRIORITY_CLASS,
                               win32process.ABOVE_NORMAL_PRIORITY_CLASS,
                               win32process.HIGH_PRIORITY_CLASS,
                               win32process.REALTIME_PRIORITY_CLASS]
            handle = win32api.OpenProcess(win32con.PROCESS_ALL_ACCESS, True, process.pid)
            win32process.SetPriorityClass(handle, priorityclasses[NICE_LVL])
        else:
            #def set_nices():#< @todo
            #    os.nice(NICE_LVL)
            #    p = psutil.Process(os.getpid())
            #    priorityclasses = [ psutil.IO
            #    p.set_ionice(psutil.IOPRIO_CLASS_IDLE)
            process = subprocess.Popen(
                command, shell=shell, cwd=cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=lambda : os.nice(NICE_LVL)
            )
        communicate_return = process.communicate()
        if process.returncode != 0:
            logging.error('Command returned an error!\n\n%s\n\n%s\n\n' % (str(command), str(communicate_return)))
        return (process.returncode,communicate_return[0],communicate_return[1])

    @staticmethod
    def md5_sum(file_path):
        fh = open(file_path, 'rb')
        m = hashlib.md5()
        while True:
            data = fh.read(8192)
            if not data:
                break
            m.update(data)
        return m.hexdigest()
    @staticmethod
    def parse_video_settings(file_path):
        transcode_settings = {}
        dom = xml.dom.minidom.parse(file_path)
        for settings_group in dom.getElementsByTagName('settings_group'):
            if settings_group.getAttribute('disabled') != '1':
                settings_group_name = settings_group.getAttribute('name').strip()
                value = settings_group.getAttribute('value')
                if value:
                    transcode_settings[settings_group_name] = value.strip()
                else:
                    try:
                        transcode_settings[settings_group_name]
                    except KeyError:
                        transcode_settings[settings_group_name] = {}
                    for setting in settings_group.getElementsByTagName('setting'):
                        if setting.getAttribute('disabled') != '1':
                            setting_name = setting.getAttribute('name').strip()
                            if setting.hasChildNodes():
                                transcode_settings[settings_group_name][setting_name] = setting.firstChild.nodeValue.strip()
                            else:
                                transcode_settings[settings_group_name][setting_name] = True
        return transcode_settings
    @staticmethod
    def compare_tracks(demuxed_files):
        '''
            Make sure the tracks aren't the same, if they are, toss out the highest numbered
        '''
        duped,track_sizes = [],{}
        for file_path in demuxed_files:
            file_size = os.stat(file_path).st_size
            try:
                track_sizes[file_size]
            except KeyError:
                track_sizes[file_size] = []
            track_sizes[file_size].append(file_path)
        for tracks in track_sizes.iteritems():
            if len(tracks[1]) != 1:
                md5s = []
                for track in tracks[1]:
                    md5 = transcode.md5_sum(track)
                    if md5 not in md5s:
                        md5s.append(md5)
                    else:
                        print 'Dupe %s' % track
                        duped.append(track)
        return duped
    @staticmethod
    def choose_track_order(media_info):
        '''
            Choose track order, default track should be the first occurrence of each type
            
            @param  Dict    media_info  Media info as returned by self.media_info()
            
            @return List    List of tracks in order
        '''
        non_english,english,track_order = [],[],[]
        for track_type in transcode.track_type_order:
            try:
                for track_id in media_info['id_maps'][track_type]:
                    try:
                        if media_info['tracks'][track_id]['Language'] == transcode.native_language[0]:
                            english.append(track_id)
                        else:
                            non_english.append(track_id)
                    except KeyError:
                        print media_info['tracks'][track_id]
                    except IndexError:
                        print '\r\n\r\n%s\r\n%s\r\n\r\n'%(track_id,media_info)
            except KeyError:
                pass
        for track_id in english:
            track_order.append(track_id)
        for track_id in non_english:
            track_order.append(track_id)
        return track_order
    
    @staticmethod
    def media_info(file_path):
        '''
            Return merged video info (mkvmerge+mediainfo apps)
            
            @param  String  file_path
            
            @return Dict    Media Info
        '''
        mkvmerge    =   transcode.mkvmerge_identify(file_path)
        mediainfo   =   transcode.mediainfo(file_path)
        track_maps = {}
        for track_id, track in mkvmerge.iteritems():
            try:
                track_maps[track['number']] = track
            except KeyError:
                for i in xrange(0,100):
                    try:
                        track_maps[str(i)]
                    except KeyError:
                        track_maps[str(i)] = track
                        break
        for track in mediainfo['tracks']:
            try:
                for attr, value in track_maps[track['ID']].iteritems():
                    track[attr] = value
            except KeyError: pass
        return mediainfo
    
    @staticmethod
    def mkvmerge_identify(file_path):
        '''
            Return the MKV Merge related info. This will be used to merge into data from media_info
            
            @param  String  file_path   file path
            
            @return Dict    {MKVMerge_TrackID}{number,uid,codec_id,..}
        '''
        info_regex = re.compile('Track ID ([0-9]{1,2}): (\w+) \((.+)\) \[(([\w/]+:[\w/\+]+ ?)*)\]')
        info = transcode.command_with_priority( [ MKVMERGE_PATH, '--identify-verbose', file_path ] )[1]
        info_out = {}
        for match in info_regex.finditer(info):
            #   1-Track Id, 2-Track Type, 3-Codec ID, 4-Verbose Attrs
            #if not len(info_out) == int(match.group(1)): raise Exception('ID Mismatch. Arr Cnt %s MKVmerge ID %s' % (len(info_out), match.group(1)))
            try: raise Exception('Double assigning %s' % info_out[match.group(1)])
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
        
    @staticmethod
    def mediainfo(file_path):
        '''
            Return info from mediainfo app
            
            @param  String  file_path   file path
            
            @return Dict    {tracks:[],id_maps{type:[]}} multi dimensional awesomeness
        '''
        dom = xml.dom.minidom.parseString(
            transcode.command_with_priority(
                [MEDIAINFO_PATH, '--Output=XML', '-f', '%s' % file_path],
            )[1]
        )
        tracks,track_types = [],{}
        for track in dom.getElementsByTagName('track'):
            chapter_info,track_info = [],{}
            track_type = track.getAttribute('type')
            for info in track.getElementsByTagName('*'):
                if info.nodeName == 'Unknown':
                    chapter_info.append(info.firstChild.nodeValue)
                else:
                    track_info[info.nodeName] = info.firstChild.nodeValue
            track_info['Menu'] = chapter_info
            try:
                try:
                    track_types[track_type]
                except KeyError:
                    track_types[track_type] = []
                track_types[track_type].append(int(track_info['ID']))
                track_info['track_type'] = track_type
                if track_type == 'Video':
                    try:
                        track_info['Bit_rate']
                    except KeyError:
                        track_info['Bit_rate'] = tracks[0]['Overall_bit_rate']
                for regex,extension in transcode.file_extension_map['regexes'].iteritems():
                    if re.search(regex,track_info['Codec_ID']) is not None:
                        track_info['extension'] = extension
                        break
                else:
                    track_info['extension'] = transcode.file_extension_map['failsafes'][track_type]
            except KeyError:
                pass
            try:
                for i in xrange(len(tracks),int(track_info['ID'])):
                    tracks.append({'fake_bitch':True})
                tracks.append(track_info)
            except KeyError:
                tracks.append(track_info)
        try:
            if len(track_types['Video'])>1:
                raise Exception('No Video Track In Input File')
        except KeyError:
            raise Exception( 'media_info len(track_types[Video]>1) \r\n%s\r\n%s\r\n%s' % (tracks,track_types,file_path) ) 
        return {'tracks':tracks,'id_maps':track_types}
    
    @staticmethod
    def transcode(old_file,new_file,media_info,new_settings={},dry_run=False):
        log_file = os.path.basename(old_file)+'.log'
        self.cleanup_files.append(log_file) #< Fix #21
        cmd = [FFMPEG_LOCATION, u'-i', '"'+old_file+'"', u'-passlogfile' , '"'+log_file+'"' ]
        win_cmd = [FFMPEG_LOCATION, u'-i', old_file, u'-passlogfile' , log_file ]
        transcode_settings = transcode.transcode_settings
        height = int(media_info['Height'].replace(' pixels','').replace(' ',''))
        width = int(media_info['Width'].replace(' pixels','').replace(' ',''))
        for setting_type,setting_group in new_settings.iteritems():
            for setting_name,setting_value in setting_group.iteritems():
                transcode_settings[setting_type][setting_name] = setting_value
        if not transcode_settings['br_percent']:
            br_modifier = 65 if height>480 or width>720 else 85
        else:
            br_modifier = transcode_settings['br_percent']
        br = media_info['Bit_rate'].rsplit(' ',1)
        br = float(br[0].replace(' ','')) * transcode.to_bit_map[br[1]]
        transcode_settings['avconv']['b:v'] = int(round(float(br)*(float(br_modifier)/100)))
        for setting_name,setting_value in transcode_settings['avconv'].iteritems():
            if setting_value:
                cmd.append(u'-%s' % setting_name)
                win_cmd.append(u'-%s' % setting_name)
                if not setting_value == True:
                    cmd.append(unicode(setting_value))
                    win_cmd.append(unicode(setting_value))
                #cmd.append(' '.join(new_cmd))
        ''' @todo   x264 settings passthru   '''
        #for setting_name,setting_value in transcode_settings['x264'].iteritems():
        #    if setting_value:
        #        new_cmd = ['-%s' % setting_name]
        #        if not setting_value == True:
        #            new_cmd.append(str(setting_value))
        #        cmd.append(' '.join(new_cmd))
        if transcode_settings['fix_dvds']:
            cmd.append(transcode.fix_dvds_cmd(height,width))
            win_cmd.append(transcode.fix_dvds_cmd(height,width))
        if transcode_settings['deinterlace']:
            cmd.append('-vf yadif')
            win_cmd.append('-vf yadif')
        first_cmd = cmd[:]
        first_win_cmd = win_cmd[:]
        first_cmd.extend( [u'-pass', u'1', u'-f', u'rawvideo', u'-y', DEV_NULL] )
        first_win_cmd.extend( [u'-pass', u'1', u'-f', u'rawvideo', u'-y', DEV_NULL] )
        cmd.extend([u'-pass', u'2', '"'+new_file+'"'])
        win_cmd.extend([u'-pass', u'2', '"'+new_file+'"'])
        logging.debug( '%s %s' % (first_cmd, cmd))
        logging.info('Transcoding (1st Pass).')
        if dry_run:
            return new_file
        else:
            cwd = os.path.dirname(new_file)
            if WINDOWS:
                if transcode.command_with_priority(
                    first_win_cmd, cwd=cwd
                    #shell=True, cwd=os.path.dirname(new_file),
                )[0] == 0: #< 1st pass success
                    logging.info('Transcoding (2nd Pass).')
                    if transcode.command_with_priority(
                        win_cmd, cwd=cwd
                        #shell=True, cwd=os.path.dirname(new_file),
                    )[0] == 0: #< 2nd pass success
                        return new_file
            else:
                if transcode.command_with_priority(
                    [unicode(' ').join(first_cmd)],
                    shell=True, cwd=os.path.dirname(new_file),
                )[0] == 0: #< 1st pass success
                    logging.info('Transcoding (2nd Pass).')
                    if transcode.command_with_priority(
                        [unicode(' ').join(cmd)],
                        shell=True, cwd=os.path.dirname(new_file),
                    )[0] == 0: #< 2nd pass success
                        return new_file
    @staticmethod
    def fix_dvds_cmd(height,width):
        return '-s %sX%s -vf crop=%s:%s:0:%s' % (width,height,width,
                                                 height-(height*.25),
                                                 height*.125)
    
    @staticmethod
    def demux(file_path, media_info, out_path, dry_run=False):
        '''
            Demux file
            
            @param  String  file_path   File path
            @param  Dict    media_info  Media info as returned by self.media_info()
            
            @return List    List of extracted tracks
        '''
        cmd = [MKVEXTRACT_PATH, u'tracks', file_path]
        demuxed = []
        for track in media_info['tracks']:
            try:
                cmd.append(u'%s:%s'%(track['ID'],
                                     os.path.join(track['ID'],out_path,
                                            u'%s%s.%s'%(os.path.basename(file_path),
                                                        track['ID'],track['extension']))))
                demuxed.append(os.path.join(out_path,
                                            u'%s%s.%s'%(os.path.basename(file_path),
                                                        track['ID'],track['extension'])))
            except KeyError:
                pass
        logging.debug( cmd )
        logging.info('Demuxing.')
        if dry_run:
            return demuxed
        else:
            if transcode.command_with_priority(cmd, cwd=out_path)[0] == 0:
                return demuxed
    @staticmethod
    def remux(mux_files,media_info,new_file,dups=[],track_order=False,dry_run=False):
        '''
            Remux
            
            @param  List    mux_files   List of demuxed files
            @param  Dict    media_info  As returned by self.media_info
            @param  Str     new_file    New file path
            @param  List    dups        Output from transcode.compare_tracks()
            @param  List    track_order List of track order (transcode.choose_track_order())
            @param  Bool    dry_run     Testing? 
            
            @return String  New file path
        '''
        logging.debug('In Remux')
        try:
            movie_name = media_info['tracks'][0]['Movie_name']
        except KeyError:
            movie_name = new_file.rsplit(os.path.sep,3).pop(2)
        cmd = [MKVMERGE_PATH, u'-o', new_file, u'--title', u"%s" % movie_name]
        #cwd = os.path.dirname(mux_files[0])
        if track_order:
            logging.debug('In Track Order')
            default_tracks = []
            for track_id in track_order:
                track_type = media_info['tracks'][track_id]['track_type'].lower()
                __track_id = '1' if track_type == 'Video' else '0'
                cmd.extend( [u'--language', u'%s:%s' % (__track_id,media_info['tracks'][track_id]['language']) ] )
                try:
                    cmd.extend( [u'--track-name', u'%s:%s' % (__track_id,media_info['tracks'][track_id]['Title'])])
                except KeyError:
                    cmd.extend( [u'--track-name', u'%s:%s' % (__track_id,media_info['tracks'][track_id]['language'])])
                if track_type not in default_tracks:
                    cmd.extend([u'--default-track', u'%s:yes' % __track_id])
                    default_tracks.append(track_type)
                cmd.append(u'%s' % mux_files.pop(0).replace('.sub','.idx'))
        else:
            for i in xrange(0,len(mux_files)):
                #   media_info['tracks'][i+1]   :   mux_files[i]
                if mux_files[i] not in dups:
                    cmd.extend( [u'--language', u'0:%s' % media_info['tracks'][i+1]['language'], u'%s' % mux_files[i] ] )
        logging.debug( ' '.join(cmd) )
        logging.info('Remuxing.')
        if transcode.command_with_priority(cmd)[0] == 0:
            with open(new_file) as f: pass #< Validate file exists
            return new_file
        
#transcode('/media/Motherload/newVideoTest/21.mkv')
#exit()
#transcode('/media/Motherload/2-renamed/')
#transcode('/media/Motherload/2-video_processing/')
#transcode('/media/Motherload/fucked/')
