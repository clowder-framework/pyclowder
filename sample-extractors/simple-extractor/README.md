# Simple Extractor

The goal of the simple extractor is to make writing of an extractor as easy as possible. It wraps almost all of the 
complexities in itself and exposes only two environment variables: ```EXTRACTION_FUNC``` and ```EXTRACTION_MODULE```. Environment 
variable ```EXTRACTION_FUNC``` is the name of the method that needs to be called when this extractor receives a message from 
the message broker. The other environment variable ```EXTRACTION_MODULE``` is the module name of python file where ```EXTRACTION_FUNC``` function has been declared.
 
# When to Use This

1. This simple extractor is meant to be used in those situations when there is already some Python code available that 
needs to be wrapped as an extractor as quickly as possible.
2. This extractor CURRENTLY outputs JSON format metadata or a list of preview files. If your extractor generates 
any additional information like generated files, datasets, collections, thumbnails, etc., this method cannot be use and 
you have to write your extractor the normal way using [PyClowder2](https://opensource.ncsa.illinois.edu/bitbucket/projects/CATS/repos/pyclowder2/browse)
3. [Docker](https://www.docker.com/) is the recommended way of developing / wrapping your code using the Simple Extractor.

## Steps for Writing an Extractor Using the Simple Extractor

To write an extractor using the Simple Extractor, you need to have your Python program available. The main function of 
this Python program is supposed to take an input file path as its parameter. It needs to return a Python dictionary that 
can contain either metadata information ("metadata"), details about file previews ("previews") or both. For example:

``` json
{   
    "metadata": dict(),
    "previews": array() 
}
```

1. Let's call your main Python program file ```your_python_program.py``` and the main function ```your_main_function```.

2. Let's create a Dockerfile for your extractor. Dockerfile contents need to be:

```Dockerfile
FROM clowder/extractors-simple-extractor:onbuild
ENV EXTRACTION_FUNC="your_main_function"
ENV EXTRACTION_MODULE="your_python_program"
```
