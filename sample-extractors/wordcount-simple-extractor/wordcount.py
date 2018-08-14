import subprocess


def wordcount(input_file_path):
    """
    This function calculates the number of lines, words, and characters in a text format file.

    :param input_file_path: Full path to the input file
    :return: Result dictionary containing metadata about lines, words, and characters in the input file
    """

    # Execute word count command on the input file and obtain the output
    result = subprocess.check_output(['wc', input_file_path], stderr=subprocess.STDOUT)

    # Split the output string into lines, words, and characters
    (lines, words, characters, _) = result.split()

    # Create metadata dictionary
    metadata = {
        'lines': lines,
        'words': words,
        'characters': characters
    }

    # Store metadata in result dictionary
    result = {
        'metadata': metadata
    }

    # Return the result dictionary
    return result
