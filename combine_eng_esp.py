

def get_files_to_export(export_dir):
    files = []
    for f in export_dir.iterdir():
        files.append(f)

    return files


def classify_files(eng_files, esp_files):
    comb_file_list = {}

    # 1 = has pair, 2 = no pair
    for f in eng_files:
        for efile in esp_files:
            if f.name == efile.name:
                comb_file_list[f] = 1
                eng_files.remove(f)
                esp_files.remove(efile)

    for f in eng_files:
        comb_file_list[f] = 2

    for f in esp_files:
        comb_file_list[f] = 3

    return comb_file_list


def combine_files(eng_file, esp_file):
    import pandas as pd
    export_location = r'{}'.format(eng_file.parent.parent.parent)
    export_filename = 'No_merged_{}'.format(eng_file.name)
    eng = pd.read_csv(eng_file, low_memory=False)
    esp = pd.read_csv(esp_file, low_memory=False)
    merged = eng.append(esp, sort=True)
    merged.to_csv('{}/{}'.format(export_location, export_filename))

def handle_file_list(comb_file_list, export_dir):
    from shutil import copy
    for f in comb_file_list:
        if comb_file_list[f] == 1:
            eng_file = f
            esp_file = export_dir / 'Spanish' / f.name
            combine_files(eng_file, esp_file)
        elif comb_file_list[f] == 2:
            copy(str(f), str(export_dir.parent))
        elif comb_file_list[f] == 3:
            pass
            copy(str(f), str(export_dir.parent))


def main(argv, cwd):
    export_dir = cwd / argv[1] / 'temp'
    eng_dir = export_dir / 'English'
    esp_dir = export_dir / 'Spanish'
    eng_files = get_files_to_export(eng_dir)
    esp_files = get_files_to_export(esp_dir)

    comb_file_list = classify_files(eng_files, esp_files)
    handle_file_list(comb_file_list, export_dir)


if __name__ == "__main__":
    def _script():
        from sys import argv
        from pathlib import Path

        cwd = Path(".")

        main(argv, cwd)

    _script()

