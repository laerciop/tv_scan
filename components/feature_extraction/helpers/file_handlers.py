"""Module to organize File Handling functions."""

from gcsfs import GCSFileSystem



class FileHandler():
    """Class to get file and save it to GCP."""
    def __init__(self, gc_project):
        self.file_system = GCSFileSystem(project=gc_project)

    def spot_gather_helper(self, path, lpath):
        """Function to get file and save it to GCP."""
        self.file_system.get(path, lpath)

if __name__ == "__main__":
    pass
