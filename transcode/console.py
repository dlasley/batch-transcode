#/usr/bin/env python

import subprocess
import os
from transcode.exceptions import CommandException

if os.name == 'nt':
    
    import win32api
    import win32process
    import win32con
    
    NICE_LVL = 0
    DEV_NULL = u'NUL'
    WINDOWS = True
    
else:
    
    #IO_NICE = 3 #< @todo, implement IO Nice
    NICE_LVL = 19
    DEV_NULL = u'/dev/null'
    WINDOWS = False


class Console(object):
    
    def __init__(self, encoding='UTF-8'):
        self.encoding = encoding
    
    def command_with_priority(command, shell=False, cwd='./', ):
        '''
        Runs a command using subprocess. Sets the priority.
        
        @param  command str     Command to execute
        @param  shell   bool    Use shell
        @param  cwd     str     Current working directory
        @return tuple   (returncode, stdout, stderr)
        '''
        
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
            
            #def set_nices():#< @TODO
            #    os.nice(NICE_LVL)
            #    p = psutil.Process(os.getpid())
            #    priorityclasses = [ psutil.IO
            #    p.set_ionice(psutil.IOPRIO_CLASS_IDLE)
            
            process = subprocess.Popen(
                command, shell=shell, cwd=cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=lambda: os.nice(NICE_LVL)
            )
            
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            raise CommandException(
                'Received non-zero exit code (%d)' % process.returncode,
                command, stdout, stderr
            )
            
        return process.returncode, stdout, stderr
    
