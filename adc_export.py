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


def main(get_config,
         bootstrap_form='form_selection',
         # TODO: provide user with the ability to choose format
         file_format='csv',
         chunk_size=50):
    # TODO: Allow users to provide multiple PIDs
    pid, bs_proj, data_proj, open_dest = get_config()

    bs_data = bs_proj.export_records(format='json', forms=[bootstrap_form])

    log.info('Initiating export related to %s bootstrap records for pid:%s',
             len(bs_data), pid)

    for row in bs_data:
        field_names = tuple(row['fieldnames'].split(','))
        file_name = (row['formname']
                     if row['filename'] is None or row['filename'] == ''
                     else row['filename'])

        # Fix to include def_field in form exports (ref: #3426).
        if field_names == ('',):
            field_names = (data_proj.def_field,) + field_names

        op_file = open_dest(file_name, file_format)

        record_list = data_proj.export_records(fields=[data_proj.def_field])
        records = list(set([str(r[data_proj.def_field]) for r in record_list]))
        # From:http://pycap.readthedocs.org/en/latest/deep.html#working-with-files # noqa
        try:
            log.info('Initiating export of data for pid:%s, form:%s ',
                     pid, row['formname'])
            header_written = False
            log.info('Records: %s', records)
            for record_chunk in chunks(records, chunk_size):
                log.info('Chunk: %s to %s', record_chunk[0], record_chunk[-1])
                data = data_proj.export_records(records=record_chunk,
                                                format=file_format,
                                                forms=[row['formname'], ],
                                                fields=field_names,
                                                event_name='unique')
                if data is None:
                    break
                # remove the header of the CSV
                data = data.split('\n', 1)[1] if header_written else data
                op_file.write(data.encode('utf-8'))
                header_written = True
            op_file.close()

        except RedcapError:
            msg = "Automatic REDCap API chunked export failed"
            log.error('Chunked export failed for pid:%s, form:%s ',
                      pid, row['formname'])
            raise ValueError(msg)

        else:
            log.info('Completed the export of data for pid:%s, form:%s ',
                     pid, row['formname'])


def chunks(l, n):
    # From:http://pycap.readthedocs.org/en/latest/deep.html#working-with-files
    for i in xrange(0, len(l), n):
        yield l[i:i + n]


def mk_get_config(os_path, openf, argv, Project):
    '''Attenuate file, network access.

    get_config() provides only
    - config files given as CLI arg 1
    - pid from CLI arg 2
      - access to REDCap projects specified by config and pid
      - write access to `file_dest` option from this config and pid
    '''
    def get_config():
        [config_fn, pid] = argv[1:3]

        config = configparser.SafeConfigParser()
        config_fp = openf(config_fn)
        config.readfp(config_fp, filename=config_fn)

        api_url = config.get('api', 'api_url')
        verify_ssl = config.getboolean('api', 'verify_ssl')
        log.debug('API URL: %s', api_url)

        bs_token = config.get(pid, 'bootstrap_token')
        log.debug('bootstrap token: %s...%s', bs_token[:4], bs_token[-4:])
        bs_proj = Project(api_url, bs_token, verify_ssl=verify_ssl)
        data_token = config.get(pid, 'data_token')
        data_proj = Project(api_url, data_token, verify_ssl=verify_ssl)

        def open_dest(file_name, file_format):
            file_dest = config.get(pid, 'file_dest')
            return openf(os_path.join(file_dest,
                                      file_name + '.' + file_format), 'wb')

        return pid, bs_proj, data_proj, open_dest
    return get_config


if __name__ == '__main__':
    def _set_logging(logfile='redcap_api_export.log'):
        from sys import argv

        FORMAT = '%(asctime)-15s - %(message)s'
        if '--debug' in argv:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(filename=logfile, format=FORMAT,
                                filemode='a', level=logging.INFO)

    def _trusted_main():
        from sys import argv
        from os import path as os_path
        from __builtin__ import open as openf
        from redcap import Project

        get_config = mk_get_config(os_path, openf, argv, Project)
        main(get_config)

    _set_logging()
    _trusted_main()
