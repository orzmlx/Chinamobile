import os

from utils import common_utils


def get_demand_pys():
    pys_files = ['C:\\Users\\No.1\\Desktop\\pytools\\run.py']
    current_path = os.getcwd()
    dirs = os.listdir(current_path)
    for d in dirs:
        if os.path.isdir(d):
            pys = common_utils.find_file(d, '.py')
            pys_files.extend([os.path.join(current_path, str(py)) for py in pys])
    return pys_files


if __name__ == "__main__":
    print(get_demand_pys())
