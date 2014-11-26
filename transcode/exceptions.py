#/usr/bin/env python
'''
Custom exception classes
'''

class CommandException(EnvironmentError):
    def __init__(self, msg, cmd, stdout, stderr, ):
        '''
        @param  str     msg     Error Message
        @param  list    cmd     Command list as sent to subprocess
        @param  str     stdout  Stdout stream from program
        @param  str     stderr  Stderr stream from program
        '''
        self.msg = msg
        self.cmd = cmd
        self.stdout = stdout
        self.stderr = stderr
        
    def __repr__(self, ):
        return 'CommandException(msg="%s", cmd=%r, stdout="%s", stderr="%s")' % (
            self.msg, self.cmd, self.stdout, self.stderr
        )
    
    def __str__(self, ):
        return self.msg
