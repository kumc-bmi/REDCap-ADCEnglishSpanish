#   Copyright (c) 2019 University of Kansas Medical Center


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


def combine_files(eng_file, esp_file):
    import pandas as pd
    export_location = r'{}'.format(eng_file.parent.parent.parent)
    eng = pd.read_csv(eng_file, low_memory=False)
    esp = pd.read_csv(esp_file, low_memory=False)
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

