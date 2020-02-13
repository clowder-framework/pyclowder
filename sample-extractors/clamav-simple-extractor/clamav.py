import datetime
import json
import re
import subprocess


def database_info():
    clamav_db = {'bytecode': {'version': 0, 'sigs': 0, 'built': None},
                 'daily': {'version': 0, 'sigs': 0, 'built': None},
                 'main': {'version': 0, 'sigs': 0, 'built': None},
                 'signatures': 0}
    result = subprocess.check_output(['c:\program files\clamav\clamconf'])
    for line in result.split(b"\n"):
        if line.startswith(b'bytecode.cvd: '):
            m = re.match(b'^bytecode.cvd: version (\d+), sigs: (\d+), built on (.*)$', line)
            if len(m.groups()) == 3:
                clamav_db['bytecode']['version'] = int(m.group(1))
                clamav_db['bytecode']['sigs'] = int(m.group(2))
                clamav_db['bytecode']['built'] = datetime.datetime.strptime(m.group(3).rstrip().decode('utf-8'), '%a %b %d %H:%M:%S %Y').isoformat()
        elif line.startswith(b'daily.cvd: '):
            m = re.match(b'^daily.cvd: version (\d+), sigs: (\d+), built on (.*)$', line)
            if len(m.groups()) == 3:
                clamav_db['daily']['version'] = int(m.group(1))
                clamav_db['daily']['sigs'] = int(m.group(2))
                clamav_db['daily']['built'] = datetime.datetime.strptime(m.group(3).rstrip().decode('utf-8'), '%a %b %d %H:%M:%S %Y').isoformat()
        elif line.startswith(b'main.cvd: '):
            m = re.match(b'^main.cvd: version (\d+), sigs: (\d+), built on (.*)$', line)
            if len(m.groups()) == 3:
                clamav_db['main']['version'] = int(m.group(1))
                clamav_db['main']['sigs'] = int(m.group(2))
                clamav_db['main']['built'] = datetime.datetime.strptime(m.group(3).rstrip().decode('utf-8'), '%a %b %d %H:%M:%S %Y').isoformat()
        elif line.startswith(b'Total number of signatures: '):
            m = re.match(b'^Total number of signatures: (\d+.)$', line)
            if len(m.groups()) == 1:
                clamav_db['signatures'] = int(m.group(1))
    return clamav_db


def scan_file(file):
    clamav_scan = {'infected': False, 'virus': None}
    try:
        subprocess.check_output(['c:\\program files\\clamav\\clamscan', '--no-summary', file])
    except subprocess.CalledProcessError as e:
        clamav_scan['infected'] = True
        clamav_scan['virus'] = e.output.replace(file + ': ', '').strip()
    return clamav_scan


result = {'database': database_info(),
          'scan': scan_file('c:\\program files\\clamav\\freshclam.conf')}
print(json.dumps(result, indent=4))