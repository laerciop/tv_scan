"""Module to organize spot transcription functions."""

import whisper


class TranscriptorModel():
    def __init__(self, model_type):
        self.model = whisper.load_model(model_type)
        
    def transcriptor_helper(self, file):
        """Function to get file and save it to GCP."""
        result = self.model.transcribe(file)
        return result

if __name__ == "__main__":
    pass
