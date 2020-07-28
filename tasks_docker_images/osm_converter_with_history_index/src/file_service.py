import errno
import os


def make_dir_for_file_if_not_exists(filename):
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise


def file_name_from_path(file_path):
    if "/" in file_path:
        return file_path.split("/")[-1]
    else:
        return file_path


def file_name_without_ext(file_name):
    if "." in file_name:
        return file_name.split(".")[0]
    else:
        return file_name
