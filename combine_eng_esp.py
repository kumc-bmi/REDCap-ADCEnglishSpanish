'''combine_eng_esp -- this file combines english csv and spanish csv export.
Usage::
  $ python capture_query.py QUERY_NAME 'query description'
  $ python combine_eng_esp.py export/
export/temp/English will have English ADC export
export/temp/Spanish will have Spanish ADC export
:copyright: Copyright 2010-2019 University of Kansas Medical Center
__ https://informatics.kumc.edu/work/wiki/REDCap
'''
import pandas as pd

def get_files_to_export(export_dir):
    files = []
    for f in export_dir.iterdir():
        files.append(f)

    return files


def handle_files(eng_files, esp_files, export_dir):
    from shutil import copy
    from os.path import isfile
    comb_files = {}

    print "Combining Files"
    for f in eng_files:
        for efile in esp_files:
            if f.name == efile.name:
                combine_files(f, efile)
                comb_files[str(f)] = str(f)

    print "Moving English Files"
    for f in eng_files:
        if not isfile("{}/{}".format(str(export_dir.parent), f.name)):
            copy(str(f), str(export_dir.parent))

    print "Moving Spanish Files"
    for f in esp_files:
        if not isfile("{}/{}".format(str(export_dir.parent), f.name)):
            copy(str(f), str(export_dir.parent))


def sanitize_value(w):
    x = str(w).decode('unicode_escape').encode('ascii','replace')
    y = x.decode('utf-8')
    z = []
    l = len(y)
    char = '?'
      
    for i in range(len(y)):
        if (y[i] == char and i != (l-1) and
           i != 0 and y[i + 1] != char and y[i-1] != char):
            z.append(y[i])
              
        elif y[i] == char:
            if ((i != (l-1) and y[i + 1] == char) and
               (i != 0 and y[i-1] != char)):
                z.append(y[i])
                  
        else:
            z.append(y[i])
          
    return ("".join(i for i in z))


def combine_files(eng_file, esp_file):

    '''
    >>> from pathlib import Path
    >>> from hashlib import md5

    >>> eng_path = Path("testcases/eng/test.csv")
    >>> esp_path = Path("testcases/esp/test.csv")

    >>> combine_files(eng_path, esp_path)

    >>> output_path = Path ("test.csv")
    >>> md5(output_path.open().read()).hexdigest()
    'bf4eebe4f4f1bae86e39a987f99f15fa'
    '''

    export_location = r'{}'.format(eng_file.parent.parent.parent)
    eng = pd.read_csv(eng_file, low_memory=False,dtype=str, keep_default_na=False)
    esp = pd.read_csv(esp_file, low_memory=False,dtype=str, keep_default_na=False)
    eng_column_list = list(eng)
    esp_column_list = list(esp)
    for i in eng_column_list:
        eng[i] = eng[i].apply(sanitize_value)
    for i in esp_column_list:
        esp[i] = esp[i].apply(sanitize_value)
    merged = eng.append(esp, sort=True)
    merged_filename = '{}/{}'.format(export_location, eng_file.name)
    merged = merged[merged.columns]
    spanish_cols = set(merged.columns.tolist()) - set(eng.columns.tolist())
    merged = merged[eng.columns.tolist() + list(spanish_cols)]
    merged.to_csv(merged_filename, index=False)


def main(argv, cwd):
    export_dir = cwd / argv[1] / 'temp'
    eng_dir = export_dir / 'English'
    esp_dir = export_dir / 'Spanish'
    eng_files = get_files_to_export(eng_dir)
    esp_files = get_files_to_export(esp_dir)

    handle_files(eng_files, esp_files, export_dir)


if __name__ == "__main__":
    def _script():
        from sys import argv
        from pathlib import Path

        cwd = Path(".")

        main(argv, cwd)

    _script()
