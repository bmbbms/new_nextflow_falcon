"""Most bash-scripting is generated here.
"""
from __future__ import absolute_import

BASH = '/bin/bash'
BUG_avoid_Text_file_busy = True
# http://stackoverflow.com/questions/1384398/usr-bin-perl-bad-interpreter-text-file-busy/


def write_sub_script(ofs, script):
    # We use shebang + chmod so we can see the sub-script in 'top'.
    # In order to avoid '/bin/bash: bad interpreter: Text file busy',
    # we 'touch' the sub-script after chmod.
    #   http://superuser.com/questions/934300/bin-bash-bad-interpreter-text-file-busy-even-though-the-file-editor-closed
    ofs.write('#!{}\n'.format(BASH))
    ofs.write('set -vex\n')
    ofs.write(script)

    if BUG_avoid_Text_file_busy:
        exe = BASH
    else:
        # We prefer to run via shebang b/c we want the script-name to appear to 'top',
        # but some users have a problem with that, e.g.
        #   https://github.com/PacificBiosciences/FALCON/issues/269
        # Another idea never worked reliably:
        # chmod +x {sub_script_bfn}
        # touch {sub_script_bfn}
        # We are trying to avoid this problem:
        #   /bin/bash: bad interpreter: Text file busy
        exe = ''
    return exe


def write_script(script, script_fn, job_done_fn=None):
    if job_done_fn:
        script += '\ntouch {}\n'.format(job_done_fn)
    with open(script_fn, 'w') as ofs:
        exe = write_sub_script(ofs, script)
