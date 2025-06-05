from zipfile import ZipFile
from fastapi import UploadFile

def extract_zip_content_stream(zip_file: UploadFile):
    """
    Extracts the content of a zip file as a stream (for large files)
    :param zip_file: The content of the zip file.
    :return: A generator yielding tuples containing the name and content of each file in the zip file.
    """
    with ZipFile(zip_file.file) as zip_ref:
        for name in zip_ref.namelist():
            content = zip_ref.read(name)
            try:
                yield name, content.decode("utf-8")
            except UnicodeDecodeError:
                yield name, content


def zip_to_dict(zip_file: bytes) -> dict:
    """
    Converts a zip file to a dictionary.
    zip_file: The content of the zip file.
    return: A dictionary containing the content of the zip file.
    """
    output = {}

    for name, content in extract_zip_content_stream(zip_file):
        output[name] = content
    return output
