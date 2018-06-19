import subprocess


def wordcount(input_file):
    result = subprocess.check_output(['wc', input_file], stderr=subprocess.STDOUT)
    (lines, words, characters, _) = result.split()
    metadata = {
        'lines': lines,
        'words': words,
        'characters': characters
    }
    result = {
        'metadata': metadata
    }
    return result
