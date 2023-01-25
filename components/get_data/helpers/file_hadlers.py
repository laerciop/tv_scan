"""Module to organize File Handling functions."""

import urllib.request
import tempfile
from gcsfs import GCSFileSystem



class FileHandler():
    """Class to get file and save it to GCP."""
    def __init__(self, gc_project):
        self.file_system = GCSFileSystem(project=gc_project)

    def spot_download_helper(self, url, dest_name):
        """Function to get file and save it to GCP."""
        dest_file = 'tv_spots_store/'+dest_name
        self.file_system.touch(dest_file)
        with self.file_system.open(dest_file, mode='wb') as file:
            with tempfile.NamedTemporaryFile() as bin_file:
                try:
                    urllib.request.urlretrieve(url, bin_file.name)
                    data = bin_file.read()
                    file.write(data)
                except Exception as e:
                    return str(e)
            file_url = file.info()
        return (dest_file, file_url)

if __name__ == "__main__":
    pass
