import urllib.request
import json
import subprocess

branch = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).decode('utf-8').strip()
subprocess.check_call(['git', 'commit', '-am', 'fix: fix catastrophic frontend UI overlapping and silent FAISS embedding DB failure'])
subprocess.check_call(['git', 'push', 'origin', 'HEAD'])

