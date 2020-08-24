r'''data_export_api -- Export data from REDCap projects into CSV files

Usage:
   python data_export_api.py example.ini 11

Based on: pioneers/active_studies/study_refresh.py
and http://pycap.readthedocs.org/en/latest/deep.html#working-with-files

Boostrap project structure is based on DataExportBoostrap_DataDictionary.csv

>>> form_info = [
...     {'formname': 'demographics', 'fieldnames': 'age,height',
...      'events': 'e1',
...      'filename': ''},
... ]

For example, a demographics instrument might have data such as:

>>> demographics = [
...     [('age', 'e1', '32'), ('id', 'e1', 'p1'), ('height', 'e1', '174')],
...     [('age', 'e2', '33'), ('id', 'e2', 'p2'), ('height', 'e2', '170')],
... ]

We configure API keys and such as follows:

>>> files = {'/home/jenkins/conf.ini': u"""
... [api]
... api_url = https://redcap/api
... verify_ssl = False
...
... [123]
... bootstrap_token = bstok123
... data_token = dtok123
... file_dest = export
... """}
>>> io = MockIO('/home/jenkins', files, form_info, demographics)

Then we use this script as follows:

    >>> argv = 'adc_export conf.ini 123'.split()
    >>> main(argv, io.cwd(), io.mkProject)

And we get the relevant data exported to the requested files:

    >>> for filename, contents in io._write.iteritems():
    ...     print '==== ', filename
    ...     print contents.replace('\r\n', '\n'),
    ====  /home/jenkins/export/demographics.csv
    age,id,height
    32,p1,174

'''

import configparser
import logging
from redcap import RedcapError

from pathlib import PurePosixPath

log = logging.getLogger(__name__)


def main(argv, cwd, mkProject):
    [config_fn, pid] = argv[1:3]

    config = configparser.SafeConfigParser()
    config.readfp((cwd / config_fn).open(), filename=config_fn)

    pid, bs_proj, data_proj, dest_dir = get_config(config, pid, cwd, mkProject)

    for form_name, file_name, field_names in form_selection(
            bs_proj, pid, data_proj.def_field):
        dest = (dest_dir / file_name).with_suffix('.csv')
        export_form(data_proj, pid, form_name, field_names, dest)


def form_selection(bs_proj, pid, def_field,
                   bootstrap_form='form_selection'):
    bootstrap_records = bs_proj.export_records(format='json',
                                               forms=[bootstrap_form])
    log.info('Initiating export related to %s bootstrap records for pid:%s',
             len(bootstrap_records), pid)

    for form_info in bootstrap_records:
        # Fix to include def_field in form exports (ref: #3426).
        if form_info['fieldnames'] == '':
            field_names = [def_field]
        else:
            field_names = form_info['fieldnames'].split(',')
        form_name = form_info['formname']
        file_name = form_info['filename'] or form_name
        yield form_name, file_name, field_names


def export_form(data_proj, pid, form_name, field_names, dest):
    log.info('Initiating export of data for pid:%s, form:%s ',
             pid, form_name)
    header_written = False
    with dest.open('w') as op_file:
        for data in csv_chunks(data_proj, pid, form_name, field_names):
            if header_written:
                data = data.split('\n', 1)[1]
            else:
                header_written = True
            op_file.write(data.encode('utf-8'))

    log.info('Completed the export of data for pid:%s, form:%s ',
             pid, form_name)


def csv_chunks(data_proj, pid, form_name, field_names,
               chunk_size=50):
    # From:http://pycap.readthedocs.org/en/latest/deep.html#working-with-files # noqa
    record_ids = data_proj.export_records(fields=[data_proj.def_field])
    no_dups = list(set([r[data_proj.def_field] for r in record_ids]))
    try:
        log.info('Records: %s', no_dups)
        for record_chunk in chunks(no_dups, chunk_size):
            log.info('Chunk: %s to %s', record_chunk[0], record_chunk[-1])
            data = data_proj.export_records(records=record_chunk,
                                            format='csv',
                                            forms=[form_name],
                                            fields=field_names,
                                            event_name='unique')
            if data:
                yield data

    except RedcapError:
        log.error('Chunked export failed for pid:%s, form:%s ',
                  pid, form_name)
        raise


def chunks(items, n):
    # From:http://pycap.readthedocs.org/en/latest/deep.html#working-with-files
    for i in xrange(0, len(items), n):
        yield items[i:i + n]


def get_config(config, pid, cwd, mkProject):
    api_url = config.get('api', 'api_url')
    verify_ssl = config.getboolean('api', 'verify_ssl')
    log.debug('API URL: %s', api_url)

    bs_token = config.get(pid, 'bootstrap_token')
    log.debug('bootstrap token: %s...%s', bs_token[:4], bs_token[-4:])
    bs_proj = mkProject(api_url, bs_token, verify_ssl=verify_ssl)
    data_token = config.get(pid, 'data_token')
    data_proj = mkProject(api_url, data_token, verify_ssl=verify_ssl)

    dest_dir = cwd / config.get(pid, 'file_dest')

    return pid, bs_proj, data_proj, dest_dir


class MockIO(object):
    def __init__(self, cwd, files, form_info, data):
        self._cwd = cwd
        self.mkProject = lambda *args, **kwargs: MockProject(form_info, data)
        MockPath._write = self._write = {}
        MockPath._read = files

    def cwd(self):
        return MockPath(self._cwd)


class MockPath(PurePosixPath):
    _write = {}
    _read = {}

    def open(self, mode='r'):
        from io import BytesIO, StringIO
        if mode == 'r':
            return StringIO(self._read[str(self)])
        else:
            out = BytesIO()
            saved = out.close

            def close():
                self._write[str(self)] = out.getvalue()
                saved()
            out.close = close
            return out

    def joinpath(self, other):
        return MockIO(str(self / other))


class MockProject(object):
    def __init__(self, form_info, data):
        self._form_info = form_info
        self._data = data
        self.def_field = 'id'

    def export_records(self,
                       format='json',
                       forms=[], fields=[], records=[], event_name=''):
        if 'form_selection' in forms:
            return self._form_info
        elif format == 'json':
            data = [{field: value
                    for (field, _event, value) in record }
                    for record in self._data]
            return [{'id': rec['id'] for rec in data}]
        elif format == 'csv':
            from csv import DictWriter
            from io import BytesIO
            data = [{field: value
                    for (field, _event, value) in record }
                    for record in self._data]
            out = BytesIO()
            dw = DictWriter(out, data[0].keys())
            dw.writeheader()
            dw.writerows(data)
            serialized = out.getvalue()
            return serialized


if __name__ == '__main__':
    def _set_logging(logfile='redcap_api_export.log'):
        from sys import argv

        FORMAT = '%(asctime)-15s - %(message)s'
        if '--debug' in argv:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(filename=logfile, format=FORMAT,
                                filemode='a', level=logging.INFO)

    def _script():
        from sys import argv
        from pathlib import Path
        from redcap import Project

        main(argv,
             cwd=Path('.'),
             mkProject=lambda *args: Project(*args))

    _set_logging()
    _script()
