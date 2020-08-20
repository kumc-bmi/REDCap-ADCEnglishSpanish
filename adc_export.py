'''data_export_api -- Export data from REDCap projects into CSV files

Usage:
   python data_export_api.py example.ini 11

Based on: pioneers/active_studies/study_refresh.py
and http://pycap.readthedocs.org/en/latest/deep.html#working-with-files

Boostrap project structure is based on DataExportBoostrap_DataDictionary.csv
'''

import configparser
import logging
from redcap import RedcapError

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
