class RemoteFileHandler:
    def __init__(self):
        self.created_files = set()

    def add_file(self, file_name: str) -> None:
        if file_name is not None:
            self.created_files.add(file_name)
