#!/usr/bin/python
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
from pprint import pprint

class transcode:
    dry_runs        =   {
        'demux'     :   False,
        'transcode' :   False,
        'remux'     :   False
    } 
    nice_lvl        =   19 
    out_dir         =   '/media/Motherload/2-transcoding/'
    encode_dir      =   '%sencodeBox/' % out_dir
    finished_dir    =   '%sfinished/' % out_dir
    pipe_output     =   open('/home/dlasley/transcode.out','w')
    dir_permissions =   0777
    track_type_order=   ('Video','Audio','Text')
    lng_codes_doc   =   '/var/www/dave_media_dev/video_manipulation/lng_codes.txt'
    settings_file = 'video_settings.xml'
    transcode_settings = {
        'x264'      :   {
                            
                        },
        'avconv'    :   {
                            'vcodec'    :   'libx264',
                            'preset'    :   'placebo', #  Ultrafast, superfast, veryfast, faster, fast, medium, slow, slower, veryslow, placebo
                            'an'        :   True,   #   No audio
                            'y'         :   True,   #   Overwrite
                            'sn'        :   True,   #   No subs
                            'tune'      :   'film',
                            'level'     :   '41',
                        },
        'fix_dvds'  :   False,
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
    THREADS = 4
    def __init__(self,indir,debug=False):
        if not os.path.isdir(self.out_dir):
            os.mkdir(self.out_dir, self.dir_permissions)
        if not os.path.isdir(self.encode_dir):
            os.mkdir(self.encode_dir, self.dir_permissions)
        if not os.path.isdir(self.finished_dir):
            os.mkdir(self.finished_dir, self.dir_permissions)
        self.__debug = debug
        self.encode_directory(indir)
        self.cleanup_files = []
        #self.encode_it(indir,'d')

    def encode_directory(self,inpath):
        '''
            Encodes an entire directory
            
            @param  String  inpath  Directory to encode
            
            @return List    Newly created files
        '''
        self.new_files = []
        self.worker_threads = {}
        def thread(root,new_root,file_name,extension,transcode_settings):
            self.new_files.append( self.encode_it('%s/%s%s'%(root,file_name,extension), '%s/%s%s' % (new_root, file_name, extension),transcode_settings) )
            del self.worker_threads[file_name]
            #os.unlink('%s/%s%s'%(root,file_name,extension))
            #logging.debug('DELETED: %s/%s%s'%(root,file_name,extension))
        for root, dirs, files in os.walk(inpath):
            dirs.sort()
            files.sort()
            if '.Apple' not in root:
                new_root = '%s%s' % (self.finished_dir,root.replace(inpath,''))
                if not os.path.isdir(new_root):
                    os.mkdir(new_root)
                try:
                    with open('%s/%s'%(root,self.settings_file)) as f: pass
                    transcode_settings = self.parse_video_settings('%s/%s'%(root,self.settings_file))
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
                print new_files
    def encode_it(self,file_path,new_file,transcode_settings={}):
        '''
            Encode wrapper, whole process
            
            @param  String  file_path   In file
            
            @return String  Out file
        '''
        media_info = self.media_info(file_path)
        cleanup_files, lng_codes = [],{}
        doc = open(self.lng_codes_doc,'r')
        for row in doc.read().split('\n'):
            cols = row.split('|')
            lng_codes[cols[3]] = cols[0]
        demuxed = self.demux(file_path,media_info,dry_run=self.dry_runs['demux'])
        cleanup_files.extend(demuxed)
        duped = self.compare_tracks(demuxed)
        #media_info['tracks'][i+1]   :   mux_files[i]
        for vid_id in media_info['id_maps']['Video']:
            demuxed[vid_id-1] = self.transcode(
                file_path,
                '%s%s-trans.%s' % (
                    self.encode_dir,
                    threading.currentThread().getName().encode('utf-8'),
                    self.transcode_settings['container']
                ),
                media_info['tracks'][vid_id],
                dry_run=self.dry_runs['transcode']
            )
            cleanup_files.append(demuxed[vid_id-1])
        track_order = self.choose_track_order(media_info)
        new_file = transcode.remux(demuxed,media_info,lng_codes,new_file,duped,track_order,dry_run=self.dry_runs['remux'])
        for cleanup_file in cleanup_files:
            print 'Removing %s' % cleanup_file
            os.remove(cleanup_file)
        return new_file
    '''     Static Methods      '''
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
                    md5 = subprocess.check_output(['md5sum', track],preexec_fn=lambda : os.nice(transcode.nice_lvl)).split(' ')
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
            track_maps[track['number']] = track
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
        info = subprocess.check_output( [ 'mkvmerge', '--identify-verbose', file_path ] )
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
            subprocess.check_output(
                ['mediainfo', '--Output=XML', '%s' % file_path],
                preexec_fn=lambda : os.nice(transcode.nice_lvl)
            )
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
                exit('Holy Crap I Never Would Have Expected To See This Error.')
        except KeyError:
            print 'media_info len(track_types[Video]>1) \r\n%s\r\n%s\r\n%s' % (tracks,track_types,file_path)
            exit(0)
        return {'tracks':tracks,'id_maps':track_types}
    @staticmethod
    def transcode(old_file,new_file,media_info,new_settings={},dry_run=False):
        print media_info
        cmd = [u'avconv', u'-i', unicode('"'+old_file+'"', "utf-8"), u'-passlogfile' , u'"'+threading.currentThread().getName().encode('utf-8')+u'"' ]
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
                if not setting_value == True:
                    cmd.append(unicode(setting_value))
                #cmd.append(' '.join(new_cmd))
        ''' @todo   x264 settings passthru   '''
        #for setting_name,setting_value in transcode_settings['x264'].iteritems():
        #    if setting_value:
        #        new_cmd = ['-%s' % setting_name]
        #        if not setting_value == True:
        #            new_cmd.append(str(setting_value))
        #        cmd.append(' '.join(new_cmd))
        if transcode_settings['fix_dvds']   :  cmd.append(transcode.fix_dvds_cmd(height,width))
        first_cmd = cmd[:]
        first_cmd.extend( [u'-pass', u'1', u'-f', u'rawvideo', u'-y', u'/dev/null'] )
        cmd.extend([u'-pass', u'2', unicode('"'+new_file+'"', "utf-8")])
        if dry_run:
            print first_cmd, cmd
            return new_file
        else:
            print first_cmd, cmd
            if subprocess.check_call([unicode(' ').join(first_cmd)],shell=True,cwd=transcode.encode_dir,stdout=transcode.pipe_output,preexec_fn=lambda : os.nice(transcode.nice_lvl)) == 0:
                if subprocess.check_call([unicode(' ').join(cmd)],shell=True,cwd=transcode.encode_dir,stdout=transcode.pipe_output,preexec_fn=lambda : os.nice(transcode.nice_lvl)) == 0:
                    return new_file
    @staticmethod
    def fix_dvds_cmd(height,width):
        return '-s %sX%s -vf crop=%s:%s:0:%s' % (width,height,width,height-(height*.25),height*.125)
    @staticmethod
    def demux(file_path, media_info, dry_run=False):
        '''
            Demux file
            
            @param  String  file_path   File path
            @param  Dict    media_info  Media info as returned by self.media_info()
            
            @return List    List of extracted tracks
        '''
        cmd = [u'mkvextract', u'tracks', unicode(file_path, "utf-8")]
        demuxed = []
        for track in media_info['tracks']:
            try:
                cmd.append(u'%s:%s%s%s.%s'%(track['ID'],transcode.encode_dir,threading.currentThread().getName().encode('utf-8'),track['ID'],track['extension']))
                demuxed.append(u'%s%s%s.%s'%(transcode.encode_dir,threading.currentThread().getName().encode('utf-8'),track['ID'],track['extension']))
            except KeyError:
                pass
        print cmd
        if dry_run:
            return demuxed
        else:
            if subprocess.check_call(cmd,cwd=transcode.encode_dir,stdout=transcode.pipe_output,preexec_fn=lambda : os.nice(transcode.nice_lvl)) == 0:
                return demuxed
    @staticmethod
    def remux(mux_files,media_info,lng_codes,new_file,dups=[],track_order=False,dry_run=False):
        '''
            Remux
            
            @param  List    mux_files   List of demuxed files
            @param  Dict    media_info  As returned by self.media_info
            @param  Dict    lng_codes   Language codes- englishName=key, ISO639-2=value
            
            @return String  New file path
        '''
        print new_file
        try:
            movie_name = media_info['tracks'][0]['Movie_name']
        except KeyError:
            movie_name = new_file.rsplit('/',3).pop(2)
        cmd = [u'mkvmerge', u'-o', unicode(new_file, "utf-8"), u'--title', u"%s" % movie_name]
        if track_order:
            default_tracks = []
            for track_id in track_order:
                track_type = media_info['tracks'][track_id]['track_type'].lower()
                __track_id = '1' if track_type == 'Video' else '0'
                cmd.extend( [u'--language', u'%s:%s' % (__track_id,lng_codes[media_info['tracks'][track_id]['Language']]) ] )
                try:
                    cmd.extend( [u'--track-name', u'%s:%s' % (__track_id,media_info['tracks'][track_id]['Title'])])
                except KeyError:
                    cmd.extend( [u'--track-name', u'%s:%s' % (__track_id,media_info['tracks'][track_id]['Language'])])
                if track_type not in default_tracks:
                    cmd.extend([u'--default-track', u'%s:yes' % __track_id])
                    default_tracks.append(track_type)
                cmd.append(u'%s' % mux_files.pop(0).replace('.sub','.idx'))
        else:
            for i in xrange(0,len(mux_files)):
                #   media_info['tracks'][i+1]   :   mux_files[i]
                if mux_files[i] not in dups:
                    cmd.extend( [u'--language', u'0:%s' % lng_codes[media_info['tracks'][i+1]['Language']], u'%s' % mux_files[i] ] )
        print ' '.join(cmd)
        if subprocess.check_call(cmd,cwd=transcode.encode_dir,stdout=transcode.pipe_output,preexec_fn=lambda : os.nice(transcode.nice_lvl)) == 0:
            with open(new_file) as f: pass
            return new_file
#transcode('/media/Motherload/newVideoTest/21.mkv')
#exit()
transcode('/media/Motherload/newVideoTest1/')
#transcode('/media/Motherload/2-video_processing/')
#transcode('/media/Motherload/fucked/')
